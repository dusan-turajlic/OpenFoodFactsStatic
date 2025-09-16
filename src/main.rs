use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ---- Config ----
const INPUT_FILE: &str = "food_facts_raw_data/products.csv.gz";
const PRODUCTS_DIR: &str = "output/static/products";
const INDEX_DIR: &str = "output/static/indexes";
const CATALOG_PATH: &str = "output/static/indexes/catalog.jsonl.gz";

const CSV_SEPARATOR: u8 = b'\t';
const PAGE_SIZE: usize = 500;

// ---- Data Structures ----
#[derive(Debug, Serialize, Deserialize)]
struct Product {
    code: String,
    product_name: Option<String>,
    brands: Option<String>,
    main_category: Option<String>,
    macros: Macros,
}

#[derive(Debug, Serialize, Deserialize)]
struct Macros {
    serving_size: Option<f64>,
    serving_quantity: Option<f64>,
    serving_unit: Option<String>,
    serving: ServingMacros,
    per100g: Per100gMacros,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexMacros {
    kcal: Option<f64>,
    serving_size: Option<f64>,
    serving_unit: Option<String>,
    fbr: Option<f64>,
    c: Option<f64>,
    f: Option<f64>,
    p: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServingMacros {
    energy_kcal: Option<f64>,
    energy_kj: Option<f64>,
    carbohydrates: Option<f64>,
    fat: Option<f64>,
    proteins: Option<f64>,
    sugars: Option<f64>,
    fiber: Option<f64>,
    salt: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Per100gMacros {
    energy_kcal: Option<f64>,
    energy_kj: Option<f64>,
    carbohydrates: Option<f64>,
    fat: Option<f64>,
    proteins: Option<f64>,
    sugars: Option<f64>,
    fiber: Option<f64>,
    salt: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexItem {
    c: String,
    n: Option<String>,
    b: Option<String>,
    p: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexPage {
    tag: String,
    page: usize,
    page_size: usize,
    total_pages: Option<usize>,
    count: Option<usize>,
    items: Vec<IndexItem>,
    prev: Option<String>,
    next: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexMeta {
    tag: String,
    count: usize,
    page_size: usize,
    total_pages: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogEntry {
    code: String,
    name: Option<String>,
    creator: Option<String>,
    brand: Option<String>,
    main_category: Option<String>,
    path: String,
    categories: Vec<String>,
    macros: IndexMacros,

}

// ---- Helpers ----
fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("Failed to create directory: {:?}", path))
}

fn slug(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| match c {
            'a'..='z' | '0'..='9' | ':' | '-' => c,
            ' ' => '-',
            _ => '-',
        })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn shard_path(base: &Path, key_slug: &str) -> PathBuf {
    let shard = if key_slug.len() >= 2 {
        &key_slug[..2]
    } else {
        "_x"
    };
    base.join(shard).join(key_slug)
}

fn to_num(v: Option<&str>) -> Option<f64> {
    let v = v?;
    let cleaned = v.replace(' ', "").replace(',', ".");
    cleaned.parse().ok().filter(|n: &f64| n.is_finite())
}

fn parse_tags(raw: Option<&str>) -> Vec<String> {
    let raw = match raw {
        Some(s) => s,
        None => return Vec::new(),
    };
    
    raw.split(|c| c == '|' || c == ',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

fn parse_serving(row: &HashMap<String, String>) -> (Option<f64>, Option<f64>, Option<String>) {
    let raw_size_str = row.get("serving_size").map(|s| s.as_str());
    let raw_size = to_num(raw_size_str);
    let mut qty = to_num(row.get("serving_quantity").map(|s| s.as_str()));
    
    // If quantity missing, try to extract from serving_size string
    if qty.is_none() {
        if let Some(size_str) = raw_size_str {
            let re = Regex::new(r"([\d.,]+)\s*(g|gram|grams|ml|milliliter|milliliters)?").unwrap();
            if let Some(captures) = re.captures(size_str) {
                if let Some(num_str) = captures.get(1) {
                    let cleaned = num_str.as_str().replace(',', ".");
                    if let Ok(parsed) = cleaned.parse::<f64>() {
                        qty = Some(parsed);
                    }
                }
            }
        }
    }
    
    // Extract unit from the original string
    let unit = raw_size_str
        .and_then(|size_str| {
            let re = Regex::new(r"\b(ml|milliliters?|g|grams?)\b").unwrap();
            re.captures(size_str)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_lowercase())
        });
    
    (raw_size, qty, unit)
}

// ---- Index Management ----
struct PagedIndex {
    cursors: Arc<Mutex<HashMap<String, (usize, usize)>>>, // (page, count)
    buffers: Arc<Mutex<HashMap<String, Vec<IndexItem>>>>,
    totals: Arc<Mutex<HashMap<String, usize>>>,
    base_dir: PathBuf,
}

impl PagedIndex {
    fn new(base_dir: PathBuf) -> Self {
        Self {
            cursors: Arc::new(Mutex::new(HashMap::new())),
            buffers: Arc::new(Mutex::new(HashMap::new())),
            totals: Arc::new(Mutex::new(HashMap::new())),
            base_dir,
        }
    }
    
    fn dir_for(&self, key: &str) -> Result<(PathBuf, String)> {
        let key_slug = slug(key);
        let dir = shard_path(&self.base_dir, &key_slug);
        ensure_dir(&dir)?;
        Ok((dir, key_slug))
    }
    
    fn page_path(&self, dir: &Path, page: usize) -> PathBuf {
        dir.join(format!("page-{:04}.json", page))
    }
    
    fn add(&self, key: String, item: IndexItem) -> Result<()> {
        let mut buffers = self.buffers.lock().unwrap();
        let mut totals = self.totals.lock().unwrap();
        
        let buf = buffers.entry(key.clone()).or_insert_with(Vec::new);
        buf.push(item);
        *totals.entry(key.clone()).or_insert(0) += 1;
        
        if buf.len() >= PAGE_SIZE {
            drop(buffers);
            drop(totals);
            self.flush_page(&key)?;
        }
        
        Ok(())
    }
    
    fn flush_page(&self, key: &str) -> Result<()> {
        let mut buffers = self.buffers.lock().unwrap();
        let mut cursors = self.cursors.lock().unwrap();
        
        let buf = buffers.remove(key).unwrap_or_default();
        if buf.is_empty() {
            return Ok(());
        }
        
        let (page, count) = cursors.get(key).copied().unwrap_or((0, 0));
        let new_page = page + 1;
        let new_count = count + buf.len();
        cursors.insert(key.to_string(), (new_page, new_count));
        
        let (dir, _) = self.dir_for(key)?;
        let page_path = self.page_path(&dir, new_page);
        
        let payload = IndexPage {
            tag: key.to_string(),
            page: new_page,
            page_size: PAGE_SIZE,
            total_pages: None,
            count: None,
            items: buf,
            prev: if new_page > 1 {
                Some(format!("page-{:04}.json", new_page - 1))
            } else {
                None
            },
            next: None,
        };
        
        let file = File::create(&page_path)
            .with_context(|| format!("Failed to create page file: {:?}", page_path))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &payload)
            .with_context(|| format!("Failed to write page: {:?}", page_path))?;
        writer.flush()?;
        
        Ok(())
    }
    
    fn finalize(&self) -> Result<()> {
        println!("   📝 Flushing remaining buffers for {} index keys...", 
                self.buffers.lock().unwrap().len());
        
        // Flush remaining buffers
        let keys: Vec<String> = {
            let buffers = self.buffers.lock().unwrap();
            buffers.keys().cloned().collect()
        };
        
        for key in keys {
            self.flush_page(&key)?;
        }
        
        println!("   📝 Writing metadata for {} index keys...", 
                self.cursors.lock().unwrap().len());
        
        // Write metadata
        let cursors = self.cursors.lock().unwrap();
        let totals = self.totals.lock().unwrap();
        
        for (key, &(page, _)) in cursors.iter() {
            let total = totals.get(key).copied().unwrap_or(0);
            let (dir, _) = self.dir_for(key)?;
            
            let meta = IndexMeta {
                tag: key.clone(),
                count: total,
                page_size: PAGE_SIZE,
                total_pages: page.max((total + PAGE_SIZE - 1) / PAGE_SIZE),
            };
            
            let meta_path = dir.join("_meta.json");
            let file = File::create(&meta_path)
                .with_context(|| format!("Failed to create meta file: {:?}", meta_path))?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, &meta)
                .with_context(|| format!("Failed to write meta: {:?}", meta_path))?;
            writer.flush()?;
            
            // Update each page with metadata
            for p in 1..=meta.total_pages {
                let page_path = self.page_path(&dir, p);
                if !page_path.exists() {
                    continue;
                }
                
                let mut page_data: IndexPage = {
                    let file = File::open(&page_path)
                        .with_context(|| format!("Failed to open page: {:?}", page_path))?;
                    let reader = BufReader::new(file);
                    serde_json::from_reader(reader)
                        .with_context(|| format!("Failed to read page: {:?}", page_path))?
                };
                
                page_data.count = Some(meta.count);
                page_data.total_pages = Some(meta.total_pages);
                page_data.next = if p < meta.total_pages {
                    Some(format!("page-{:04}.json", p + 1))
                } else {
                    None
                };
                
                let file = File::create(&page_path)
                    .with_context(|| format!("Failed to create updated page: {:?}", page_path))?;
                let mut writer = BufWriter::new(file);
                serde_json::to_writer(&mut writer, &page_data)
                    .with_context(|| format!("Failed to write updated page: {:?}", page_path))?;
                writer.flush()?;
            }
        }
        
        Ok(())
    }
}

// ---- Main Processing ----
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting OpenFoodFacts data processing...");
    println!("📁 Input file: {}", INPUT_FILE);
    println!("📁 Products directory: {}", PRODUCTS_DIR);
    println!("📁 Index directory: {}", INDEX_DIR);
    println!("📄 Page size: {}", PAGE_SIZE);
    
    println!("\n📂 Phase 1: Setting up directories and streams...");
    ensure_dir(Path::new(PRODUCTS_DIR))?;
    ensure_dir(Path::new(INDEX_DIR))?;
    ensure_dir(Path::new(CATALOG_PATH).parent().unwrap())?;
    println!("✅ Directories created successfully");
    
    let catalog_file = File::create(CATALOG_PATH)
        .with_context(|| format!("Failed to create catalog file: {}", CATALOG_PATH))?;
    let catalog_writer = Arc::new(Mutex::new(GzEncoder::new(
        catalog_file,
        Compression::default(),
    )));
    
    let categories_index = Arc::new(PagedIndex::new(
        Path::new(INDEX_DIR).join("categories")
    ));
    let brands_index = Arc::new(PagedIndex::new(
        Path::new(INDEX_DIR).join("brands")
    ));
    println!("✅ Index structures initialized (categories & brands)");
    
    println!("\n📊 Phase 2: Starting data processing pipeline...");
    println!("📖 Reading from: {}", INPUT_FILE);
    
    let input_file = File::open(INPUT_FILE)
        .with_context(|| format!("Failed to open input file: {}", INPUT_FILE))?;
    let decoder = GzDecoder::new(input_file);
    let mut reader = ReaderBuilder::new()
        .delimiter(CSV_SEPARATOR)
        .flexible(true)  // Allow records with different field counts
        .from_reader(decoder);
    
    let mut processed_count = 0;
    let mut skipped_count = 0;
    let start_time = Instant::now();
    
    // Create progress bar
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message("Processing products...");
    
    // Get headers first before creating records iterator
    let headers = reader.headers()?.clone();
    let mut records = reader.records();
    let mut batch = Vec::new();
    const BATCH_SIZE: usize = 2000;
    
    while let Some(record) = records.next() {
        match record {
            Ok(record) => {
                batch.push(record);
            }
            Err(e) => {
                println!("⚠️  Skipping malformed record: {}", e);
                skipped_count += 1;
                continue;
            }
        }
        
        if batch.len() >= BATCH_SIZE {
            process_batch(
                &batch,
                &headers,
                &catalog_writer,
                &categories_index,
                &brands_index,
                &mut processed_count,
                &mut skipped_count,
                &pb,
            ).await?;
            batch.clear();
        }
    }
    
    // Process remaining records
    if !batch.is_empty() {
        process_batch(
            &batch,
            &headers,
            &catalog_writer,
            &categories_index,
            &brands_index,
            &mut processed_count,
            &mut skipped_count,
            &pb,
        ).await?;
    }
    
    pb.finish_with_message("Processing complete!");
    
    println!("\n🏁 Phase 3: Finalizing data processing...");
    let total_time = start_time.elapsed().as_secs_f64();
    println!("📊 Processing complete:");
    println!("   ✅ Processed: {} products", processed_count);
    println!("   ⚠️  Skipped: {} rows", skipped_count);
    println!("   ⏱️  Total time: {:.2}s", total_time);
    println!("   📈 Average rate: {} products/sec", 
            (processed_count as f64 / total_time) as usize);
    
    println!("\n📝 Phase 4: Finalizing streams and indexes...");
    println!("   🔄 Closing catalog stream...");
    {
        let mut writer = catalog_writer.lock().unwrap();
        writer.try_finish()
            .with_context(|| "Failed to finish catalog compression")?;
    }
    println!("   ✅ Catalog stream closed");
    
    println!("   🔄 Finalizing category index...");
    categories_index.finalize()?;
    println!("   ✅ Category index finalized");
    
    println!("   🔄 Finalizing brand index...");
    brands_index.finalize()?;
    println!("   ✅ Brand index finalized");
    
    println!("\n🎉 All done! Data processing pipeline completed successfully.");
    println!("📁 Check the following directories for results:");
    println!("   • Products: {}", PRODUCTS_DIR);
    println!("   • Indexes: {}", INDEX_DIR);
    println!("   • Catalog: {}", CATALOG_PATH);
    
    Ok(())
}

async fn process_batch(
    batch: &[StringRecord],
    headers: &StringRecord,
    catalog_writer: &Arc<Mutex<GzEncoder<File>>>,
    categories_index: &Arc<PagedIndex>,
    brands_index: &Arc<PagedIndex>,
    processed_count: &mut usize,
    skipped_count: &mut usize,
    pb: &ProgressBar,
) -> Result<()> {
    let results: Result<Vec<_>, anyhow::Error> = batch.par_iter().map(|record| -> Result<Option<(CatalogEntry, Vec<String>, Option<String>)>, anyhow::Error> {
        let code = record.get(0).unwrap_or("").replace(|c: char| !c.is_ascii_digit(), "");
        if code.is_empty() {
            return Ok(None);
        }
        
        let row: HashMap<String, String> = record.iter()
            .enumerate()
            .filter_map(|(i, field)| {
                headers.get(i).map(|header| (header.to_string(), field.to_string()))
            })
            .collect();
        
        let name = row.get("product_name").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        let creator = row.get("creator").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        let brand = row.get("brands").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        let main_category = row.get("main_category").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
            
        let (serving_size, serving_quantity, serving_unit) = parse_serving(&row);

        let per100g = Per100gMacros {
            energy_kcal: to_num(row.get("energy-kcal_100g").map(|s| s.as_str())),
            energy_kj: to_num(row.get("energy-kj_100g").map(|s| s.as_str())),
            carbohydrates: to_num(row.get("carbohydrates_100g").map(|s| s.as_str())),
            fat: to_num(row.get("fat_100g").map(|s| s.as_str())),
            proteins: to_num(row.get("proteins_100g").map(|s| s.as_str())),
            sugars: to_num(row.get("sugars_100g").map(|s| s.as_str())),
            fiber: to_num(row.get("fiber_100g").map(|s| s.as_str())),
            salt: to_num(row.get("salt_100g").map(|s| s.as_str())),
        };
        
        let serving = ServingMacros {
            energy_kcal: to_num(row.get("energy-kcal_serving").map(|s| s.as_str())),
            energy_kj: to_num(row.get("energy-kj_serving").map(|s| s.as_str())),
            carbohydrates: to_num(row.get("carbohydrates_serving").map(|s| s.as_str())),
            fat: to_num(row.get("fat_serving").map(|s| s.as_str())),
            proteins: to_num(row.get("proteins_serving").map(|s| s.as_str())),
            sugars: to_num(row.get("sugars_serving").map(|s| s.as_str())),
            fiber: to_num(row.get("fiber_serving").map(|s| s.as_str())),
            salt: to_num(row.get("salt_serving").map(|s| s.as_str())),
        };
        
        // Prepare index macros for validation
        let index_macros_per_100g = IndexMacros {
            kcal: per100g.energy_kcal,
            serving_size: Some(100.0),
            serving_unit: Some('g'.to_string()),
            // Need the fiber so we can calculate the net carbs
            fbr: per100g.fiber,
            c: per100g.carbohydrates,
            f: per100g.fat,
            p: per100g.proteins,
        };

        let index_macros_per_serving = IndexMacros {
            kcal: serving.energy_kcal,
            serving_size: serving_size.clone(),
            serving_unit: serving_unit.clone(),
            // Need the fiber so we can calculate the net carbs
            fbr: serving.fiber,
            c: serving.carbohydrates,
            f: serving.fat,
            p: serving.proteins,
        };

        // Check if serving macros are complete
        let serving_macros_complete = index_macros_per_serving.kcal.is_some() 
            && index_macros_per_serving.serving_size.is_some() 
            && index_macros_per_serving.serving_unit.is_some() 
            && index_macros_per_serving.fbr.is_some() 
            && index_macros_per_serving.c.is_some() 
            && index_macros_per_serving.f.is_some() 
            && index_macros_per_serving.p.is_some();
        
        // Check if per100g macros have at least basic nutritional data
        let per100g_macros_sufficient = index_macros_per_100g.kcal.is_some() 
            && (index_macros_per_100g.c.is_some() || index_macros_per_100g.f.is_some() || index_macros_per_100g.p.is_some());
        
        // Skip entry entirely if neither serving nor per100g macros are sufficient
        if !serving_macros_complete && !per100g_macros_sufficient {
            return Ok(None);
        }

        let macros = Macros {
            serving_size: serving_size.clone(),
            serving_quantity: serving_quantity.clone(),
            serving_unit: serving_unit.clone(),
            serving: serving.clone(),
            per100g: per100g.clone(),
        };
        
        let product = Product {
            code: code.clone(),
            product_name: name.clone(),
            brands: brand.clone(),
            main_category: main_category.clone(),
            macros,
        };
        
        // Write product file (only if macros are sufficient)
        let product_path = Path::new(PRODUCTS_DIR).join(format!("{}.json", code));
        let file = File::create(&product_path)
            .with_context(|| format!("Failed to create product file: {:?}", product_path))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &product)
            .with_context(|| format!("Failed to write product: {:?}", product_path))?;
        writer.flush()?;
        
        // Prepare catalog entry
        let categories = parse_tags(row.get("categories_tags").map(|s| s.as_str()));

        let macros: IndexMacros = if serving_macros_complete {
            index_macros_per_serving
        } else {
            index_macros_per_100g
        };

        let catalog_entry = CatalogEntry {
            code: code.clone(),
            name,
            creator,
            brand: brand.clone(),
            main_category,
            path: product_path.to_string_lossy().replace('\\', "/"),
            categories: categories.clone(),
            macros
        };
        
        Ok(Some((catalog_entry, categories, brand)))
    }).collect();
    
    let results = results?;
    
    // Process results sequentially for I/O operations
    for result in results {
        if let Some((catalog_entry, categories, brand)) = result {
            *processed_count += 1;
            pb.set_position(*processed_count as u64);
            
            // Write to catalog
            {
                let mut writer = catalog_writer.lock().unwrap();
                let line = serde_json::to_string(&catalog_entry)
                    .with_context(|| "Failed to serialize catalog entry")?;
                writeln!(writer, "{}", line)
                    .with_context(|| "Failed to write catalog entry")?;
            }
            
            // Add to indexes
            for category in categories {
                let item = IndexItem {
                    c: catalog_entry.code.clone(),
                    n: catalog_entry.name.clone(),
                    b: catalog_entry.brand.clone(),
                    p: catalog_entry.path.clone(),
                };
                categories_index.add(category, item)?;
            }
            
            if let Some(brand) = brand {
                let item = IndexItem {
                    c: catalog_entry.code.clone(),
                    n: catalog_entry.name.clone(),
                    b: Some(brand.clone()),
                    p: catalog_entry.path.clone(),
                };
                brands_index.add(brand, item)?;
            }
        } else {
            *skipped_count += 1;
        }
    }
    
    Ok(())
}
