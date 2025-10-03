use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use brotli::enc::BrotliEncoderParams;
use brotli::CompressorWriter;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::process::Command;
use std::os::unix::process::ExitStatusExt;
use iso3166::{Country, LIST};

// ---- Config ----
const INPUT_FILE: &str = "food_facts_raw_data/products.csv.gz";
const PRODUCTS_DIR: &str = "output/static/products";
const CATALOG_BASE_DIR: &str = "output/static/indexes/catalogs";

const CSV_SEPARATOR: u8 = b'\t';

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
struct IndexMacros {
    kcal: Option<f64>,
    serving_size: Option<f64>,
    serving_unit: Option<String>,
    fiber: Option<f64>,
    carbs: Option<f64>,
    fat: Option<f64>,
    protein: Option<f64>,
}

#[derive(Debug)]
struct CatalogEntry {
    code: String,
    name: Option<String>,
    brand: Option<String>,
    country: Option<String>,
    serving_size: Option<f64>,
    serving_unit: Option<String>,
    fiber: Option<f64>,
    carbs: Option<f64>,
    fat: Option<f64>,
    protein: Option<f64>,
}

impl serde::Serialize for CatalogEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(10))?;
        seq.serialize_element(&self.code)?;
        seq.serialize_element(&self.name)?;
        seq.serialize_element(&self.brand)?;
        seq.serialize_element(&self.country)?;
        seq.serialize_element(&self.serving_size)?;
        seq.serialize_element(&self.serving_unit)?;
        seq.serialize_element(&self.fiber)?;
        seq.serialize_element(&self.carbs)?;
        seq.serialize_element(&self.fat)?;
        seq.serialize_element(&self.protein)?;
        seq.end()
    }
}

// ---- Helpers ----
fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("Failed to create directory: {:?}", path))
}

fn check_file_descriptors() -> Result<usize> {
    let output = Command::new("lsof")
        .arg("-p")
        .arg(std::process::id().to_string())
        .output()
        .unwrap_or_else(|_| std::process::Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: Vec::new(),
            stderr: Vec::new(),
        });
    
    if output.status.success() {
        let count = String::from_utf8_lossy(&output.stdout).lines().count();
        Ok(count)
    } else {
        // Fallback: assume we're okay if lsof fails
        Ok(0)
    }
}

fn write_product_file(product: &Product, code: &str) -> Result<()> {
    let product_path = Path::new(PRODUCTS_DIR).join(format!("{}.json", code));
    
    // Create and write to file with explicit closing
    let file = File::create(&product_path)
        .with_context(|| format!("Failed to create product file: {:?}", product_path))?;
    
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, product)
        .with_context(|| format!("Failed to write product: {:?}", product_path))?;
    
    // Explicitly flush and close
    writer.flush()
        .with_context(|| format!("Failed to flush product file: {:?}", product_path))?;
    
    // Force close the file by dropping the writer
    drop(writer);
    
    Ok(())
}

fn force_file_cleanup() {
    // Force garbage collection to clean up file handles
    std::hint::black_box(());
    
    // Small delay to allow system to clean up
    std::thread::sleep(std::time::Duration::from_millis(1));
}

fn normalize_country_codes(countries_str: &str) -> Vec<String> {
    let mut codes = Vec::new();
    
    // Split by common separators
    let countries: Vec<&str> = countries_str
        .split(|c| c == ',' || c == ';' || c == '|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    
    for country in countries {
        let normalized = country.to_lowercase();
        
        // Handle special cases
        if normalized.contains("world") {
            codes.push("global".to_string());
            continue;
        }
        
        // Try to map the country name to ISO code
        let iso_code = map_country_to_iso_code(&normalized);
        
        if !codes.contains(&iso_code) {
            codes.push(iso_code);
        }
    }
    
    // If no valid countries found, default to "unknown"
    if codes.is_empty() {
        codes.push("unknown".to_string());
    }
    
    codes
}

fn map_country_to_iso_code(name: &str) -> String {
    // Handle language:country format (e.g., en:germany, fr:france, de:deutschland)
    let country_name = if let Some(colon_pos) = name.find(':') {
        let country_part = &name[colon_pos + 1..];
        // Convert hyphens to spaces for better matching
        country_part.replace('-', " ")
    } else {
        name.to_string()
    };
    
    // If it's already a 2-letter code, try to validate it
    if country_name.len() == 2 && country_name.chars().all(|c| c.is_ascii_alphabetic()) {
        if let Some(_country) = Country::from_alpha2_ignore_case(&country_name) {
            return country_name.to_lowercase();
        }
    }
    
    // Try to find by country name (case-insensitive)
    let search_name = country_name.to_lowercase();
    
    // First, try exact match with country names
    for country in LIST {
        if country.name.to_lowercase() == search_name {
            return country.alpha2.to_lowercase();
        }
    }
    
    // Then try partial matches for common variations
    for country in LIST {
        let country_name_lower = country.name.to_lowercase();
        
        // Handle common variations and aliases
        if matches_country_name(&search_name, &country_name_lower, country.alpha2) {
            return country.alpha2.to_lowercase();
        }
    }
    
    // Special handling for "world" - put in global directory
    if search_name == "world" {
        return "global".to_string();
    }
    
    "unknown".to_string()
}

fn matches_country_name(search_name: &str, country_name: &str, alpha2: &str) -> bool {
    // Handle common country name variations
    match (search_name, alpha2) {
        // United States variations
        ("united states", "US") | ("usa", "US") | ("us", "US") | ("united states of america", "US") => true,
        
        // United Kingdom variations
        ("united kingdom", "GB") | ("uk", "GB") | ("great britain", "GB") | ("britain", "GB") => true,
        
        // Germany variations
        ("germany", "DE") | ("deutschland", "DE") => true,
        
        // Netherlands variations
        ("netherlands", "NL") | ("holland", "NL") => true,
        
        // Switzerland variations
        ("switzerland", "CH") | ("schweiz", "CH") => true,
        
        // Brazil variations
        ("brazil", "BR") | ("brasil", "BR") => true,
        
        // South Korea variations
        ("south korea", "KR") | ("korea", "KR") => true,
        
        // Czech Republic variations
        ("czech republic", "CZ") | ("czechia", "CZ") => true,
        
        // Congo variations
        ("congo", "CG") | ("democratic republic of the congo", "CD") | ("drc", "CD") => true,
        
        // Cape Verde variations
        ("cape verde", "CV") | ("cabo verde", "CV") => true,
        
        // Ivory Coast variations
        ("ivory coast", "CI") | ("cote d'ivoire", "CI") => true,
        
        // Myanmar variations
        ("myanmar", "MM") | ("burma", "MM") => true,
        
        // UAE variations
        ("united arab emirates", "AE") | ("uae", "AE") => true,
        
        // French Guiana
        ("french guiana", "GF") => true,
        
        // North Macedonia
        ("north macedonia", "MK") => true,
        
        // Kosovo (not in ISO 3166 but commonly used)
        ("kosovo", "XK") => true,
        
        // Partial matches for compound names
        _ => {
            // Check if search name is contained in country name or vice versa
            country_name.contains(search_name) || search_name.contains(country_name)
        }
    }
}

fn compress_catalog_file(jsonl_path: &Path, br_path: &Path) -> Result<()> {
    // Read the uncompressed JSONL file
    let input_file = File::open(jsonl_path)
        .with_context(|| format!("Failed to open JSONL file: {:?}", jsonl_path))?;
    let reader = BufReader::new(input_file);
    
    // Create the compressed output file
    let output_file = File::create(br_path)
        .with_context(|| format!("Failed to create compressed file: {:?}", br_path))?;
    let mut writer = CompressorWriter::with_params(
        output_file,
        4096,
        &BrotliEncoderParams::default(),
    );
    
    // Copy data from input to compressed output
    let mut buffer = [0; 8192];
    let mut reader = reader;
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        writer.write_all(&buffer[..bytes_read])?;
    }
    
    writer.flush()?;
    drop(writer); // Ensure file is closed
    
    Ok(())
}

fn to_num(v: Option<&str>) -> Option<f64> {
    let v = v?;
    let cleaned = v.replace(' ', "").replace(',', ".");
    cleaned.parse().ok().filter(|n: &f64| n.is_finite())
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

// ---- Main Processing ----
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting OpenFoodFacts data processing...");
    println!("📁 Input file: {}", INPUT_FILE);
    println!("📁 Products directory: {}", PRODUCTS_DIR);
    println!("📁 Catalogs directory: {}", CATALOG_BASE_DIR);
    
    println!("\n📂 Phase 1: Setting up directories and streams...");
    ensure_dir(Path::new(PRODUCTS_DIR))?;
    ensure_dir(Path::new(CATALOG_BASE_DIR))?;
    println!("✅ Directories created successfully");
    
    // Create a map to store catalog writers for each country (uncompressed JSONL)
    let catalog_writers: Arc<Mutex<HashMap<String, BufWriter<File>>>> = Arc::new(Mutex::new(HashMap::new()));
    
    println!("✅ Catalog writers initialized");
    
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
    const BATCH_SIZE: usize = 10; // Very small batches to prevent file descriptor exhaustion
    
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
                &catalog_writers,
                &mut processed_count,
                &mut skipped_count,
                &pb,
            ).await?;
            batch.clear();
            
            // Force garbage collection and file handle cleanup
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    // Process remaining records
    if !batch.is_empty() {
        process_batch(
            &batch,
            &headers,
            &catalog_writers,
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
    
    println!("\n📝 Phase 4: Finalizing streams...");
    println!("   🔄 Closing catalog JSONL streams...");
    let country_codes: Vec<String> = {
        let mut writers = catalog_writers.lock().unwrap();
        let codes: Vec<String> = writers.keys().cloned().collect();
        for (country, writer) in writers.iter_mut() {
            writer.flush()
                .with_context(|| format!("Failed to flush catalog JSONL for country: {}", country))?;
        }
        writers.clear(); // Close all file handles
        codes
    };
    println!("   ✅ All catalog JSONL streams closed");
    
    println!("   🔄 Compressing catalog files...");
    for country_code in country_codes {
        let catalog_dir = Path::new(CATALOG_BASE_DIR).join(&country_code);
        let jsonl_path = catalog_dir.join("catalog.jsonl");
        let br_path = catalog_dir.join("catalog.jsonl.br");
        
        if jsonl_path.exists() {
            compress_catalog_file(&jsonl_path, &br_path)?;
            // Remove the uncompressed file after compression
            fs::remove_file(&jsonl_path)?;
            println!("   ✅ Compressed catalog for country: {}", country_code);
        }
    }
    println!("   ✅ All catalog files compressed");
    
    println!("\n🎉 All done! Data processing pipeline completed successfully.");
    println!("📁 Check the following directories for results:");
    println!("   • Products: {}", PRODUCTS_DIR);
    println!("   • Catalogs: {}", CATALOG_BASE_DIR);
    
    Ok(())
}

async fn process_batch(
    batch: &[StringRecord],
    headers: &StringRecord,
    catalog_writers: &Arc<Mutex<HashMap<String, BufWriter<File>>>>,
    processed_count: &mut usize,
    skipped_count: &mut usize,
    pb: &ProgressBar,
) -> Result<()> {
    // Process records sequentially to avoid too many open files
    for record in batch {
        let result = process_single_record(record, headers)?;
        
        if let Some((catalog_entries, _brand)) = result {
            *processed_count += 1;
            pb.set_position(*processed_count as u64);
            
            // Write catalog entry to each country's catalog
            for (catalog_entry, country_code) in catalog_entries {
                // Get or create catalog writer for this country
                {
                    let mut writers = catalog_writers.lock().unwrap();
                    if !writers.contains_key(&country_code) {
                        let catalog_dir = Path::new(CATALOG_BASE_DIR).join(&country_code);
                        ensure_dir(&catalog_dir)?;
                        let catalog_path = catalog_dir.join("catalog.jsonl"); // Uncompressed JSONL
                        let catalog_file = File::create(&catalog_path)
                            .with_context(|| format!("Failed to create catalog file: {:?}", catalog_path))?;
                        let writer = BufWriter::new(catalog_file);
                        writers.insert(country_code.clone(), writer);
                    }
                    
                    let writer = writers.get_mut(&country_code).unwrap();
                    let line = serde_json::to_string(&catalog_entry)
                        .with_context(|| "Failed to serialize catalog entry")?;
                    writeln!(writer, "{}", line)
                        .with_context(|| "Failed to write catalog entry")?;
                }
            }
        } else {
            *skipped_count += 1;
        }
        
        // Check file descriptor usage and add delay if needed
        if *processed_count % 10 == 0 {
            if let Ok(fd_count) = check_file_descriptors() {
                if fd_count > 500 {
                    println!("⚠️  High file descriptor usage: {} open files", fd_count);
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            }
        }
        
        // Force cleanup and delay to allow file handles to be released
        force_file_cleanup();
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    }
    
    Ok(())
}

fn process_single_record(
    record: &StringRecord,
    headers: &StringRecord,
) -> Result<Option<(Vec<(CatalogEntry, String)>, Option<String>)>> {
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
    let brand = row.get("brands").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let main_category = row.get("main_category").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let countries_str = row.get("countries").map(|s| s.trim()).unwrap_or("");
    let country_codes = normalize_country_codes(countries_str);
        
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
        serving_unit: Some("g".to_string()),
        fiber: per100g.fiber,
        carbs: per100g.carbohydrates,
        fat: per100g.fat,
        protein: per100g.proteins,
    };

    let index_macros_per_serving = IndexMacros {
        kcal: serving.energy_kcal,
        serving_size: serving_size.clone(),
        serving_unit: serving_unit.clone(),
        fiber: serving.fiber,
        carbs: serving.carbohydrates,
        fat: serving.fat,
        protein: serving.proteins,
    };

    // Check if serving macros are complete
    let serving_macros_complete = index_macros_per_serving.kcal.is_some() 
        && index_macros_per_serving.serving_size.is_some() 
        && index_macros_per_serving.serving_unit.is_some() 
        && index_macros_per_serving.fiber.is_some() 
        && index_macros_per_serving.carbs.is_some() 
        && index_macros_per_serving.fat.is_some() 
        && index_macros_per_serving.protein.is_some();
    
    // Check if per100g macros have at least basic nutritional data
    let per100g_macros_sufficient = index_macros_per_100g.kcal.is_some() 
        && (index_macros_per_100g.carbs.is_some() || index_macros_per_100g.fat.is_some() || index_macros_per_100g.protein.is_some());
    
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
    
    // Write individual product file (only if macros are sufficient)
    write_product_file(&product, &code)?;
    
    // Force cleanup of file handles
    force_file_cleanup();
    
    let macros: IndexMacros = if serving_macros_complete {
        index_macros_per_serving
    } else {
        index_macros_per_100g
    };

    // Create catalog entries for each country
    let mut catalog_entries = Vec::new();
    for country_code in &country_codes {
        let catalog_entry = CatalogEntry {
            code: code.clone(),
            name: name.clone(),
            brand: brand.clone(),
            country: Some(country_code.clone()),
            serving_size: macros.serving_size,
            serving_unit: macros.serving_unit.clone(),
            fiber: macros.fiber,
            carbs: macros.carbs,
            fat: macros.fat,
            protein: macros.protein,
        };
        catalog_entries.push((catalog_entry, country_code.clone()));
    }
    
    Ok(Some((catalog_entries, brand)))
}