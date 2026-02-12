use anyhow::{Context, Result};
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use brotli::enc::BrotliEncoderParams;
use brotli::CompressorWriter;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Instant;
use iso3166::{Country, LIST};

// ---- Config ----
const INPUT_FILE: &str = "food_facts_raw_data/products.csv.gz";
const PRODUCTS_DIR: &str = "output/static/products";
const CATALOG_BASE_DIR: &str = "output/static/indexes/catalogs";

const CSV_SEPARATOR: u8 = b'\t';
const BATCH_SIZE: usize = 10_000;

// ---- Data Structures ----
#[derive(Debug, Serialize, Deserialize)]
struct Product {
    code: String,
    product_name: Option<String>,
    generic_name: Option<String>,
    ingredients_text: Option<String>,
    brands: Option<String>,
    main_category: Option<String>,
    serving_size: Option<f64>,
    serving_unit: Option<String>,
    breakdown: Breakdown,
}

#[derive(Debug, Serialize, Deserialize)]
struct Breakdown {
    macros: MacroNutrients,
    vitamins: Vitamins,
    minerals: Minerals,
    fats: FatBreakdown,
    other: OtherNutrients,
}

#[derive(Debug, Serialize, Deserialize)]
struct MacroNutrients {
    energy_kcal: Option<f64>,
    energy_kj: Option<f64>,
    carbohydrates: Option<f64>,
    fat: Option<f64>,
    proteins: Option<f64>,
    sugars: Option<f64>,
    fiber: Option<f64>,
    salt: Option<f64>,
    added_sugars: Option<f64>,
    sucrose: Option<f64>,
    glucose: Option<f64>,
    fructose: Option<f64>,
    galactose: Option<f64>,
    lactose: Option<f64>,
    maltose: Option<f64>,
    maltodextrins: Option<f64>,
    psicose: Option<f64>,
    starch: Option<f64>,
    polyols: Option<f64>,
    erythritol: Option<f64>,
    isomalt: Option<f64>,
    maltitol: Option<f64>,
    sorbitol: Option<f64>,
    soluble_fiber: Option<f64>,
    insoluble_fiber: Option<f64>,
    polydextrose: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Vitamins {
    vitamin_a: Option<f64>,
    beta_carotene: Option<f64>,
    vitamin_d: Option<f64>,
    vitamin_e: Option<f64>,
    vitamin_k: Option<f64>,
    vitamin_c: Option<f64>,
    vitamin_b1: Option<f64>,
    vitamin_b2: Option<f64>,
    vitamin_pp: Option<f64>,
    vitamin_b6: Option<f64>,
    vitamin_b9: Option<f64>,
    folates: Option<f64>,
    vitamin_b12: Option<f64>,
    biotin: Option<f64>,
    pantothenic_acid: Option<f64>,
    choline: Option<f64>,
    phylloquinone: Option<f64>,
    inositol: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Minerals {
    sodium: Option<f64>,
    calcium: Option<f64>,
    phosphorus: Option<f64>,
    iron: Option<f64>,
    magnesium: Option<f64>,
    zinc: Option<f64>,
    copper: Option<f64>,
    manganese: Option<f64>,
    fluoride: Option<f64>,
    selenium: Option<f64>,
    chromium: Option<f64>,
    molybdenum: Option<f64>,
    iodine: Option<f64>,
    potassium: Option<f64>,
    chloride: Option<f64>,
    silica: Option<f64>,
    bicarbonate: Option<f64>,
    sulphate: Option<f64>,
    nitrate: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FatBreakdown {
    saturated: Option<f64>,
    unsaturated: Option<f64>,
    monounsaturated: Option<f64>,
    polyunsaturated: Option<f64>,
    trans: Option<f64>,
    cholesterol: Option<f64>,
    omega_3: Option<f64>,
    omega_6: Option<f64>,
    omega_9: Option<f64>,
    alpha_linolenic_acid: Option<f64>,
    eicosapentaenoic_acid: Option<f64>,
    docosahexaenoic_acid: Option<f64>,
    linoleic_acid: Option<f64>,
    arachidonic_acid: Option<f64>,
    gamma_linolenic_acid: Option<f64>,
    dihomo_gamma_linolenic_acid: Option<f64>,
    oleic_acid: Option<f64>,
    elaidic_acid: Option<f64>,
    gondoic_acid: Option<f64>,
    mead_acid: Option<f64>,
    erucic_acid: Option<f64>,
    nervonic_acid: Option<f64>,
    butyric_acid: Option<f64>,
    caproic_acid: Option<f64>,
    caprylic_acid: Option<f64>,
    capric_acid: Option<f64>,
    lauric_acid: Option<f64>,
    myristic_acid: Option<f64>,
    palmitic_acid: Option<f64>,
    stearic_acid: Option<f64>,
    arachidic_acid: Option<f64>,
    behenic_acid: Option<f64>,
    lignoceric_acid: Option<f64>,
    cerotic_acid: Option<f64>,
    montanic_acid: Option<f64>,
    melissic_acid: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OtherNutrients {
    caffeine: Option<f64>,
    taurine: Option<f64>,
    carnitine: Option<f64>,
    beta_glucan: Option<f64>,
    alcohol: Option<f64>,
    nucleotides: Option<f64>,
    casein: Option<f64>,
    serum_proteins: Option<f64>,
    methylsulfonylmethane: Option<f64>,
    energy_from_fat: Option<f64>,
    added_salt: Option<f64>,
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

// ---- Column Index ----
struct ColumnIndex {
    product_name: Option<usize>,
    generic_name: Option<usize>,
    ingredients_text: Option<usize>,
    brands: Option<usize>,
    main_category: Option<usize>,
    countries: Option<usize>,
    serving_size: Option<usize>,
    serving_quantity: Option<usize>,
    // Macros
    energy_kcal_100g: Option<usize>,
    energy_kj_100g: Option<usize>,
    carbohydrates_100g: Option<usize>,
    fat_100g: Option<usize>,
    proteins_100g: Option<usize>,
    sugars_100g: Option<usize>,
    fiber_100g: Option<usize>,
    salt_100g: Option<usize>,
    added_sugars_100g: Option<usize>,
    sucrose_100g: Option<usize>,
    glucose_100g: Option<usize>,
    fructose_100g: Option<usize>,
    galactose_100g: Option<usize>,
    lactose_100g: Option<usize>,
    maltose_100g: Option<usize>,
    maltodextrins_100g: Option<usize>,
    psicose_100g: Option<usize>,
    starch_100g: Option<usize>,
    polyols_100g: Option<usize>,
    erythritol_100g: Option<usize>,
    isomalt_100g: Option<usize>,
    maltitol_100g: Option<usize>,
    sorbitol_100g: Option<usize>,
    soluble_fiber_100g: Option<usize>,
    insoluble_fiber_100g: Option<usize>,
    polydextrose_100g: Option<usize>,
    // Vitamins
    vitamin_a_100g: Option<usize>,
    beta_carotene_100g: Option<usize>,
    vitamin_d_100g: Option<usize>,
    vitamin_e_100g: Option<usize>,
    vitamin_k_100g: Option<usize>,
    vitamin_c_100g: Option<usize>,
    vitamin_b1_100g: Option<usize>,
    vitamin_b2_100g: Option<usize>,
    vitamin_pp_100g: Option<usize>,
    vitamin_b6_100g: Option<usize>,
    vitamin_b9_100g: Option<usize>,
    folates_100g: Option<usize>,
    vitamin_b12_100g: Option<usize>,
    biotin_100g: Option<usize>,
    pantothenic_acid_100g: Option<usize>,
    choline_100g: Option<usize>,
    phylloquinone_100g: Option<usize>,
    inositol_100g: Option<usize>,
    // Minerals
    sodium_100g: Option<usize>,
    calcium_100g: Option<usize>,
    phosphorus_100g: Option<usize>,
    iron_100g: Option<usize>,
    magnesium_100g: Option<usize>,
    zinc_100g: Option<usize>,
    copper_100g: Option<usize>,
    manganese_100g: Option<usize>,
    fluoride_100g: Option<usize>,
    selenium_100g: Option<usize>,
    chromium_100g: Option<usize>,
    molybdenum_100g: Option<usize>,
    iodine_100g: Option<usize>,
    potassium_100g: Option<usize>,
    chloride_100g: Option<usize>,
    silica_100g: Option<usize>,
    bicarbonate_100g: Option<usize>,
    sulphate_100g: Option<usize>,
    nitrate_100g: Option<usize>,
    // Fats
    saturated_fat_100g: Option<usize>,
    unsaturated_fat_100g: Option<usize>,
    monounsaturated_fat_100g: Option<usize>,
    polyunsaturated_fat_100g: Option<usize>,
    trans_fat_100g: Option<usize>,
    cholesterol_100g: Option<usize>,
    omega_3_fat_100g: Option<usize>,
    omega_6_fat_100g: Option<usize>,
    omega_9_fat_100g: Option<usize>,
    alpha_linolenic_acid_100g: Option<usize>,
    eicosapentaenoic_acid_100g: Option<usize>,
    docosahexaenoic_acid_100g: Option<usize>,
    linoleic_acid_100g: Option<usize>,
    arachidonic_acid_100g: Option<usize>,
    gamma_linolenic_acid_100g: Option<usize>,
    dihomo_gamma_linolenic_acid_100g: Option<usize>,
    oleic_acid_100g: Option<usize>,
    elaidic_acid_100g: Option<usize>,
    gondoic_acid_100g: Option<usize>,
    mead_acid_100g: Option<usize>,
    erucic_acid_100g: Option<usize>,
    nervonic_acid_100g: Option<usize>,
    butyric_acid_100g: Option<usize>,
    caproic_acid_100g: Option<usize>,
    caprylic_acid_100g: Option<usize>,
    capric_acid_100g: Option<usize>,
    lauric_acid_100g: Option<usize>,
    myristic_acid_100g: Option<usize>,
    palmitic_acid_100g: Option<usize>,
    stearic_acid_100g: Option<usize>,
    arachidic_acid_100g: Option<usize>,
    behenic_acid_100g: Option<usize>,
    lignoceric_acid_100g: Option<usize>,
    cerotic_acid_100g: Option<usize>,
    montanic_acid_100g: Option<usize>,
    melissic_acid_100g: Option<usize>,
    // Other
    caffeine_100g: Option<usize>,
    taurine_100g: Option<usize>,
    carnitine_100g: Option<usize>,
    beta_glucan_100g: Option<usize>,
    alcohol_100g: Option<usize>,
    nucleotides_100g: Option<usize>,
    casein_100g: Option<usize>,
    serum_proteins_100g: Option<usize>,
    methylsulfonylmethane_100g: Option<usize>,
    energy_from_fat_100g: Option<usize>,
    added_salt_100g: Option<usize>,
}

impl ColumnIndex {
    fn from_headers(headers: &StringRecord) -> Self {
        let mut idx = ColumnIndex {
            product_name: None,
            generic_name: None,
            ingredients_text: None,
            brands: None,
            main_category: None,
            countries: None,
            serving_size: None,
            serving_quantity: None,
            energy_kcal_100g: None,
            energy_kj_100g: None,
            carbohydrates_100g: None,
            fat_100g: None,
            proteins_100g: None,
            sugars_100g: None,
            fiber_100g: None,
            salt_100g: None,
            added_sugars_100g: None,
            sucrose_100g: None,
            glucose_100g: None,
            fructose_100g: None,
            galactose_100g: None,
            lactose_100g: None,
            maltose_100g: None,
            maltodextrins_100g: None,
            psicose_100g: None,
            starch_100g: None,
            polyols_100g: None,
            erythritol_100g: None,
            isomalt_100g: None,
            maltitol_100g: None,
            sorbitol_100g: None,
            soluble_fiber_100g: None,
            insoluble_fiber_100g: None,
            polydextrose_100g: None,
            vitamin_a_100g: None,
            beta_carotene_100g: None,
            vitamin_d_100g: None,
            vitamin_e_100g: None,
            vitamin_k_100g: None,
            vitamin_c_100g: None,
            vitamin_b1_100g: None,
            vitamin_b2_100g: None,
            vitamin_pp_100g: None,
            vitamin_b6_100g: None,
            vitamin_b9_100g: None,
            folates_100g: None,
            vitamin_b12_100g: None,
            biotin_100g: None,
            pantothenic_acid_100g: None,
            choline_100g: None,
            phylloquinone_100g: None,
            inositol_100g: None,
            sodium_100g: None,
            calcium_100g: None,
            phosphorus_100g: None,
            iron_100g: None,
            magnesium_100g: None,
            zinc_100g: None,
            copper_100g: None,
            manganese_100g: None,
            fluoride_100g: None,
            selenium_100g: None,
            chromium_100g: None,
            molybdenum_100g: None,
            iodine_100g: None,
            potassium_100g: None,
            chloride_100g: None,
            silica_100g: None,
            bicarbonate_100g: None,
            sulphate_100g: None,
            nitrate_100g: None,
            saturated_fat_100g: None,
            unsaturated_fat_100g: None,
            monounsaturated_fat_100g: None,
            polyunsaturated_fat_100g: None,
            trans_fat_100g: None,
            cholesterol_100g: None,
            omega_3_fat_100g: None,
            omega_6_fat_100g: None,
            omega_9_fat_100g: None,
            alpha_linolenic_acid_100g: None,
            eicosapentaenoic_acid_100g: None,
            docosahexaenoic_acid_100g: None,
            linoleic_acid_100g: None,
            arachidonic_acid_100g: None,
            gamma_linolenic_acid_100g: None,
            dihomo_gamma_linolenic_acid_100g: None,
            oleic_acid_100g: None,
            elaidic_acid_100g: None,
            gondoic_acid_100g: None,
            mead_acid_100g: None,
            erucic_acid_100g: None,
            nervonic_acid_100g: None,
            butyric_acid_100g: None,
            caproic_acid_100g: None,
            caprylic_acid_100g: None,
            capric_acid_100g: None,
            lauric_acid_100g: None,
            myristic_acid_100g: None,
            palmitic_acid_100g: None,
            stearic_acid_100g: None,
            arachidic_acid_100g: None,
            behenic_acid_100g: None,
            lignoceric_acid_100g: None,
            cerotic_acid_100g: None,
            montanic_acid_100g: None,
            melissic_acid_100g: None,
            caffeine_100g: None,
            taurine_100g: None,
            carnitine_100g: None,
            beta_glucan_100g: None,
            alcohol_100g: None,
            nucleotides_100g: None,
            casein_100g: None,
            serum_proteins_100g: None,
            methylsulfonylmethane_100g: None,
            energy_from_fat_100g: None,
            added_salt_100g: None,
        };
        for (i, header) in headers.iter().enumerate() {
            match header {
                "product_name" => idx.product_name = Some(i),
                "generic_name" => idx.generic_name = Some(i),
                "ingredients_text" => idx.ingredients_text = Some(i),
                "brands" => idx.brands = Some(i),
                "main_category" => idx.main_category = Some(i),
                "countries" => idx.countries = Some(i),
                "serving_size" => idx.serving_size = Some(i),
                "serving_quantity" => idx.serving_quantity = Some(i),
                // Macros
                "energy-kcal_100g" => idx.energy_kcal_100g = Some(i),
                "energy-kj_100g" => idx.energy_kj_100g = Some(i),
                "carbohydrates_100g" => idx.carbohydrates_100g = Some(i),
                "fat_100g" => idx.fat_100g = Some(i),
                "proteins_100g" => idx.proteins_100g = Some(i),
                "sugars_100g" => idx.sugars_100g = Some(i),
                "fiber_100g" => idx.fiber_100g = Some(i),
                "salt_100g" => idx.salt_100g = Some(i),
                "added-sugars_100g" => idx.added_sugars_100g = Some(i),
                "sucrose_100g" => idx.sucrose_100g = Some(i),
                "glucose_100g" => idx.glucose_100g = Some(i),
                "fructose_100g" => idx.fructose_100g = Some(i),
                "galactose_100g" => idx.galactose_100g = Some(i),
                "lactose_100g" => idx.lactose_100g = Some(i),
                "maltose_100g" => idx.maltose_100g = Some(i),
                "maltodextrins_100g" => idx.maltodextrins_100g = Some(i),
                "psicose_100g" => idx.psicose_100g = Some(i),
                "starch_100g" => idx.starch_100g = Some(i),
                "polyols_100g" => idx.polyols_100g = Some(i),
                "erythritol_100g" => idx.erythritol_100g = Some(i),
                "isomalt_100g" => idx.isomalt_100g = Some(i),
                "maltitol_100g" => idx.maltitol_100g = Some(i),
                "sorbitol_100g" => idx.sorbitol_100g = Some(i),
                "soluble-fiber_100g" => idx.soluble_fiber_100g = Some(i),
                "insoluble-fiber_100g" => idx.insoluble_fiber_100g = Some(i),
                "polydextrose_100g" => idx.polydextrose_100g = Some(i),
                // Vitamins
                "vitamin-a_100g" => idx.vitamin_a_100g = Some(i),
                "beta-carotene_100g" => idx.beta_carotene_100g = Some(i),
                "vitamin-d_100g" => idx.vitamin_d_100g = Some(i),
                "vitamin-e_100g" => idx.vitamin_e_100g = Some(i),
                "vitamin-k_100g" => idx.vitamin_k_100g = Some(i),
                "vitamin-c_100g" => idx.vitamin_c_100g = Some(i),
                "vitamin-b1_100g" => idx.vitamin_b1_100g = Some(i),
                "vitamin-b2_100g" => idx.vitamin_b2_100g = Some(i),
                "vitamin-pp_100g" => idx.vitamin_pp_100g = Some(i),
                "vitamin-b6_100g" => idx.vitamin_b6_100g = Some(i),
                "vitamin-b9_100g" => idx.vitamin_b9_100g = Some(i),
                "folates_100g" => idx.folates_100g = Some(i),
                "vitamin-b12_100g" => idx.vitamin_b12_100g = Some(i),
                "biotin_100g" => idx.biotin_100g = Some(i),
                "pantothenic-acid_100g" => idx.pantothenic_acid_100g = Some(i),
                "choline_100g" => idx.choline_100g = Some(i),
                "phylloquinone_100g" => idx.phylloquinone_100g = Some(i),
                "inositol_100g" => idx.inositol_100g = Some(i),
                // Minerals
                "sodium_100g" => idx.sodium_100g = Some(i),
                "calcium_100g" => idx.calcium_100g = Some(i),
                "phosphorus_100g" => idx.phosphorus_100g = Some(i),
                "iron_100g" => idx.iron_100g = Some(i),
                "magnesium_100g" => idx.magnesium_100g = Some(i),
                "zinc_100g" => idx.zinc_100g = Some(i),
                "copper_100g" => idx.copper_100g = Some(i),
                "manganese_100g" => idx.manganese_100g = Some(i),
                "fluoride_100g" => idx.fluoride_100g = Some(i),
                "selenium_100g" => idx.selenium_100g = Some(i),
                "chromium_100g" => idx.chromium_100g = Some(i),
                "molybdenum_100g" => idx.molybdenum_100g = Some(i),
                "iodine_100g" => idx.iodine_100g = Some(i),
                "potassium_100g" => idx.potassium_100g = Some(i),
                "chloride_100g" => idx.chloride_100g = Some(i),
                "silica_100g" => idx.silica_100g = Some(i),
                "bicarbonate_100g" => idx.bicarbonate_100g = Some(i),
                "sulphate_100g" => idx.sulphate_100g = Some(i),
                "nitrate_100g" => idx.nitrate_100g = Some(i),
                // Fats
                "saturated-fat_100g" => idx.saturated_fat_100g = Some(i),
                "unsaturated-fat_100g" => idx.unsaturated_fat_100g = Some(i),
                "monounsaturated-fat_100g" => idx.monounsaturated_fat_100g = Some(i),
                "polyunsaturated-fat_100g" => idx.polyunsaturated_fat_100g = Some(i),
                "trans-fat_100g" => idx.trans_fat_100g = Some(i),
                "cholesterol_100g" => idx.cholesterol_100g = Some(i),
                "omega-3-fat_100g" => idx.omega_3_fat_100g = Some(i),
                "omega-6-fat_100g" => idx.omega_6_fat_100g = Some(i),
                "omega-9-fat_100g" => idx.omega_9_fat_100g = Some(i),
                "alpha-linolenic-acid_100g" => idx.alpha_linolenic_acid_100g = Some(i),
                "eicosapentaenoic-acid_100g" => idx.eicosapentaenoic_acid_100g = Some(i),
                "docosahexaenoic-acid_100g" => idx.docosahexaenoic_acid_100g = Some(i),
                "linoleic-acid_100g" => idx.linoleic_acid_100g = Some(i),
                "arachidonic-acid_100g" => idx.arachidonic_acid_100g = Some(i),
                "gamma-linolenic-acid_100g" => idx.gamma_linolenic_acid_100g = Some(i),
                "dihomo-gamma-linolenic-acid_100g" => idx.dihomo_gamma_linolenic_acid_100g = Some(i),
                "oleic-acid_100g" => idx.oleic_acid_100g = Some(i),
                "elaidic-acid_100g" => idx.elaidic_acid_100g = Some(i),
                "gondoic-acid_100g" => idx.gondoic_acid_100g = Some(i),
                "mead-acid_100g" => idx.mead_acid_100g = Some(i),
                "erucic-acid_100g" => idx.erucic_acid_100g = Some(i),
                "nervonic-acid_100g" => idx.nervonic_acid_100g = Some(i),
                "butyric-acid_100g" => idx.butyric_acid_100g = Some(i),
                "caproic-acid_100g" => idx.caproic_acid_100g = Some(i),
                "caprylic-acid_100g" => idx.caprylic_acid_100g = Some(i),
                "capric-acid_100g" => idx.capric_acid_100g = Some(i),
                "lauric-acid_100g" => idx.lauric_acid_100g = Some(i),
                "myristic-acid_100g" => idx.myristic_acid_100g = Some(i),
                "palmitic-acid_100g" => idx.palmitic_acid_100g = Some(i),
                "stearic-acid_100g" => idx.stearic_acid_100g = Some(i),
                "arachidic-acid_100g" => idx.arachidic_acid_100g = Some(i),
                "behenic-acid_100g" => idx.behenic_acid_100g = Some(i),
                "lignoceric-acid_100g" => idx.lignoceric_acid_100g = Some(i),
                "cerotic-acid_100g" => idx.cerotic_acid_100g = Some(i),
                "montanic-acid_100g" => idx.montanic_acid_100g = Some(i),
                "melissic-acid_100g" => idx.melissic_acid_100g = Some(i),
                // Other
                "caffeine_100g" => idx.caffeine_100g = Some(i),
                "taurine_100g" => idx.taurine_100g = Some(i),
                "carnitine_100g" => idx.carnitine_100g = Some(i),
                "beta-glucan_100g" => idx.beta_glucan_100g = Some(i),
                "alcohol_100g" => idx.alcohol_100g = Some(i),
                "nucleotides_100g" => idx.nucleotides_100g = Some(i),
                "casein_100g" => idx.casein_100g = Some(i),
                "serum-proteins_100g" => idx.serum_proteins_100g = Some(i),
                "methylsulfonylmethane_100g" => idx.methylsulfonylmethane_100g = Some(i),
                "energy-from-fat_100g" => idx.energy_from_fat_100g = Some(i),
                "added-salt_100g" => idx.added_salt_100g = Some(i),
                _ => {}
            }
        }
        idx
    }
}

fn get_field<'a>(record: &'a StringRecord, idx: Option<usize>) -> Option<&'a str> {
    idx.and_then(|i| record.get(i)).filter(|s| !s.is_empty())
}

// ---- Cached Regex ----
fn serving_qty_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"([\d.,]+)\s*(g|gram|grams|ml|milliliter|milliliters)?").unwrap())
}

fn serving_unit_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b(ml|milliliters?|g|grams?)\b").unwrap())
}

// ---- Helpers ----
fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("Failed to create directory: {:?}", path))
}

fn write_product_file(product: &Product, code: &str) -> Result<()> {
    let product_path = Path::new(PRODUCTS_DIR).join(format!("{}.json", code));

    let file = File::create(&product_path)
        .with_context(|| format!("Failed to create product file: {:?}", product_path))?;

    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, product)
        .with_context(|| format!("Failed to write product: {:?}", product_path))?;

    writer.flush()
        .with_context(|| format!("Failed to flush product file: {:?}", product_path))?;

    Ok(())
}

// ---- Country Cache ----
fn build_country_cache() -> HashMap<String, String> {
    let mut cache = HashMap::new();

    // Insert all ISO 3166 country names (lowercase -> alpha2 lowercase)
    for country in LIST {
        cache.insert(country.name.to_lowercase(), country.alpha2.to_lowercase());
        // Also insert the alpha2 code itself
        cache.insert(country.alpha2.to_lowercase(), country.alpha2.to_lowercase());
    }

    // Insert hardcoded aliases
    let aliases: &[(&str, &str)] = &[
        // United States variations
        ("united states", "us"),
        ("usa", "us"),
        ("united states of america", "us"),
        // United Kingdom variations
        ("united kingdom", "gb"),
        ("uk", "gb"),
        ("great britain", "gb"),
        ("britain", "gb"),
        // Germany variations
        ("germany", "de"),
        ("deutschland", "de"),
        // Netherlands variations
        ("netherlands", "nl"),
        ("holland", "nl"),
        // Switzerland variations
        ("switzerland", "ch"),
        ("schweiz", "ch"),
        // Brazil variations
        ("brazil", "br"),
        ("brasil", "br"),
        // South Korea variations
        ("south korea", "kr"),
        ("korea", "kr"),
        // Czech Republic variations
        ("czech republic", "cz"),
        ("czechia", "cz"),
        // Congo variations
        ("congo", "cg"),
        ("democratic republic of the congo", "cd"),
        ("drc", "cd"),
        // Cape Verde variations
        ("cape verde", "cv"),
        ("cabo verde", "cv"),
        // Ivory Coast variations
        ("ivory coast", "ci"),
        ("cote d'ivoire", "ci"),
        // Myanmar variations
        ("myanmar", "mm"),
        ("burma", "mm"),
        // UAE variations
        ("united arab emirates", "ae"),
        ("uae", "ae"),
        // French Guiana
        ("french guiana", "gf"),
        // North Macedonia
        ("north macedonia", "mk"),
        // Kosovo
        ("kosovo", "xk"),
        // World
        ("world", "global"),
    ];

    for &(alias, code) in aliases {
        cache.insert(alias.to_string(), code.to_string());
    }

    cache
}

fn normalize_country_codes(countries_str: &str, cache: &HashMap<String, String>) -> Vec<String> {
    let mut codes = Vec::new();

    let countries: Vec<&str> = countries_str
        .split(|c| c == ',' || c == ';' || c == '|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    for country in countries {
        let normalized = country.to_lowercase();

        if normalized.contains("world") {
            if !codes.contains(&"global".to_string()) {
                codes.push("global".to_string());
            }
            continue;
        }

        let iso_code = map_country_to_iso_code(&normalized, cache);

        if !codes.contains(&iso_code) {
            codes.push(iso_code);
        }
    }

    if codes.is_empty() {
        codes.push("unknown".to_string());
    }

    codes
}

fn map_country_to_iso_code(name: &str, cache: &HashMap<String, String>) -> String {
    // Handle language:country format (e.g., en:germany, fr:france, de:deutschland)
    let country_name = if let Some(colon_pos) = name.find(':') {
        let country_part = &name[colon_pos + 1..];
        country_part.replace('-', " ")
    } else {
        name.to_string()
    };

    let search_name = country_name.to_lowercase();

    // Fast path: check cache for exact match
    if let Some(code) = cache.get(&search_name) {
        return code.clone();
    }

    // If it's already a 2-letter code, try to validate it
    if search_name.len() == 2 && search_name.chars().all(|c| c.is_ascii_alphabetic()) {
        if let Some(_country) = Country::from_alpha2_ignore_case(&search_name) {
            return search_name;
        }
    }

    // Partial-match fallback for compound names
    for country in LIST {
        let country_name_lower = country.name.to_lowercase();
        if country_name_lower.contains(&search_name) || search_name.contains(&country_name_lower) {
            return country.alpha2.to_lowercase();
        }
    }

    if search_name == "world" {
        return "global".to_string();
    }

    "unknown".to_string()
}

fn compress_catalog_file(jsonl_path: &Path, br_path: &Path) -> Result<()> {
    let input_file = File::open(jsonl_path)
        .with_context(|| format!("Failed to open JSONL file: {:?}", jsonl_path))?;
    let mut reader = BufReader::new(input_file);

    let output_file = File::create(br_path)
        .with_context(|| format!("Failed to create compressed file: {:?}", br_path))?;
    let mut writer = CompressorWriter::with_params(
        output_file,
        64 * 1024,
        &BrotliEncoderParams::default(),
    );

    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        writer.write_all(&buffer[..bytes_read])?;
    }

    writer.flush()?;
    drop(writer);

    Ok(())
}

fn to_num(v: Option<&str>) -> Option<f64> {
    let v = v?;
    let cleaned = v.replace(' ', "").replace(',', ".");
    cleaned.parse().ok().filter(|n: &f64| n.is_finite())
}

fn parse_serving(record: &StringRecord, col_index: &ColumnIndex) -> (Option<f64>, Option<f64>, Option<String>) {
    let raw_size_str = get_field(record, col_index.serving_size);
    let raw_size = to_num(raw_size_str);
    let mut qty = to_num(get_field(record, col_index.serving_quantity));

    if qty.is_none() {
        if let Some(size_str) = raw_size_str {
            if let Some(captures) = serving_qty_regex().captures(size_str) {
                if let Some(num_str) = captures.get(1) {
                    let cleaned = num_str.as_str().replace(',', ".");
                    if let Ok(parsed) = cleaned.parse::<f64>() {
                        qty = Some(parsed);
                    }
                }
            }
        }
    }

    let unit = raw_size_str
        .and_then(|size_str| {
            serving_unit_regex().captures(size_str)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_lowercase())
        });

    (raw_size, qty, unit)
}

// ---- Main Processing ----
fn main() -> Result<()> {
    println!("Starting OpenFoodFacts data processing...");
    println!("Input file: {}", INPUT_FILE);
    println!("Products directory: {}", PRODUCTS_DIR);
    println!("Catalogs directory: {}", CATALOG_BASE_DIR);

    println!("\nPhase 1: Setting up directories and streams...");
    ensure_dir(Path::new(PRODUCTS_DIR))?;
    ensure_dir(Path::new(CATALOG_BASE_DIR))?;
    println!("Directories created successfully");

    let mut catalog_writers: HashMap<String, BufWriter<File>> = HashMap::new();

    println!("Catalog writers initialized");

    // Build country cache once
    let country_cache = build_country_cache();
    println!("Country cache built ({} entries)", country_cache.len());

    println!("\nPhase 2: Starting data processing pipeline...");
    println!("Reading from: {}", INPUT_FILE);

    let input_file = File::open(INPUT_FILE)
        .with_context(|| format!("Failed to open input file: {}", INPUT_FILE))?;
    let decoder = GzDecoder::new(input_file);
    let mut reader = ReaderBuilder::new()
        .delimiter(CSV_SEPARATOR)
        .flexible(true)
        .from_reader(decoder);

    let mut processed_count: usize = 0;
    let mut skipped_count: usize = 0;
    let start_time = Instant::now();

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message("Processing products...");

    let headers = reader.headers()?.clone();
    let col_index = ColumnIndex::from_headers(&headers);

    let mut records = reader.records();
    let mut batch: Vec<StringRecord> = Vec::with_capacity(BATCH_SIZE);

    while let Some(record) = records.next() {
        match record {
            Ok(record) => {
                batch.push(record);
            }
            Err(e) => {
                println!("Warning: Skipping malformed record: {}", e);
                skipped_count += 1;
                continue;
            }
        }

        if batch.len() >= BATCH_SIZE {
            let (batch_processed, batch_skipped) = process_batch(
                &batch,
                &col_index,
                &country_cache,
                &mut catalog_writers,
            )?;
            processed_count += batch_processed;
            skipped_count += batch_skipped;
            pb.set_position(processed_count as u64);
            batch.clear();
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        let (batch_processed, batch_skipped) = process_batch(
            &batch,
            &col_index,
            &country_cache,
            &mut catalog_writers,
        )?;
        processed_count += batch_processed;
        skipped_count += batch_skipped;
        pb.set_position(processed_count as u64);
    }

    pb.finish_with_message("Processing complete!");

    println!("\nPhase 3: Finalizing data processing...");
    let total_time = start_time.elapsed().as_secs_f64();
    println!("Processing complete:");
    println!("   Processed: {} products", processed_count);
    println!("   Skipped: {} rows", skipped_count);
    println!("   Total time: {:.2}s", total_time);
    println!("   Average rate: {} products/sec",
            (processed_count as f64 / total_time) as usize);

    println!("\nPhase 4: Finalizing streams...");
    println!("   Closing catalog JSONL streams...");
    let country_codes: Vec<String> = {
        let codes: Vec<String> = catalog_writers.keys().cloned().collect();
        for (country, writer) in catalog_writers.iter_mut() {
            writer.flush()
                .with_context(|| format!("Failed to flush catalog JSONL for country: {}", country))?;
        }
        catalog_writers.clear();
        codes
    };
    println!("   All catalog JSONL streams closed");

    println!("   Compressing catalog files...");
    country_codes.par_iter().for_each(|country_code| {
        let catalog_dir = Path::new(CATALOG_BASE_DIR).join(country_code);
        let jsonl_path = catalog_dir.join("catalog.jsonl");
        let br_path = catalog_dir.join("catalog.jsonl.br");

        if jsonl_path.exists() {
            if let Err(e) = compress_catalog_file(&jsonl_path, &br_path) {
                eprintln!("Error compressing catalog for {}: {}", country_code, e);
            } else {
                let _ = fs::remove_file(&jsonl_path);
            }
        }
    });
    println!("   All catalog files compressed");

    println!("\nAll done! Data processing pipeline completed successfully.");
    println!("Check the following directories for results:");
    println!("   Products: {}", PRODUCTS_DIR);
    println!("   Catalogs: {}", CATALOG_BASE_DIR);

    Ok(())
}

fn process_batch(
    batch: &[StringRecord],
    col_index: &ColumnIndex,
    country_cache: &HashMap<String, String>,
    catalog_writers: &mut HashMap<String, BufWriter<File>>,
) -> Result<(usize, usize)> {
    // Parallel: parse records + write product files across all cores
    let results: Vec<_> = batch.par_iter()
        .filter_map(|record| {
            match process_single_record(record, col_index, country_cache) {
                Ok(Some(result)) => Some(result),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("Error processing record: {}", e);
                    None
                }
            }
        })
        .collect();

    let batch_processed = results.len();
    let batch_skipped = batch.len() - batch_processed;

    // Sequential: write catalog entries (shared file handles)
    for (catalog_entries, _brand) in &results {
        for (catalog_entry, country_code) in catalog_entries {
            if !catalog_writers.contains_key(country_code) {
                let catalog_dir = Path::new(CATALOG_BASE_DIR).join(country_code);
                ensure_dir(&catalog_dir)?;
                let catalog_path = catalog_dir.join("catalog.jsonl");
                let catalog_file = File::create(&catalog_path)
                    .with_context(|| format!("Failed to create catalog file: {:?}", catalog_path))?;
                let writer = BufWriter::with_capacity(64 * 1024, catalog_file);
                catalog_writers.insert(country_code.clone(), writer);
            }

            let writer = catalog_writers.get_mut(country_code).unwrap();
            let line = serde_json::to_string(catalog_entry)
                .with_context(|| "Failed to serialize catalog entry")?;
            writeln!(writer, "{}", line)
                .with_context(|| "Failed to write catalog entry")?;
        }
    }

    Ok((batch_processed, batch_skipped))
}

fn process_single_record(
    record: &StringRecord,
    col_index: &ColumnIndex,
    country_cache: &HashMap<String, String>,
) -> Result<Option<(Vec<(CatalogEntry, String)>, Option<String>)>> {
    let code = record.get(0).unwrap_or("").replace(|c: char| !c.is_ascii_digit(), "");
    if code.is_empty() {
        return Ok(None);
    }

    let name = get_field(record, col_index.product_name).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let generic_name = get_field(record, col_index.generic_name).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let ingredients_text = get_field(record, col_index.ingredients_text).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let brand = get_field(record, col_index.brands).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let main_category = get_field(record, col_index.main_category).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let countries_str = get_field(record, col_index.countries).unwrap_or("");
    let country_codes = normalize_country_codes(countries_str, country_cache);

    let (_serving_size_raw, _serving_quantity, serving_unit) = parse_serving(record, col_index);

    // Extract serving_size from serving_quantity fallback chain
    let serving_size = to_num(get_field(record, col_index.serving_quantity))
        .or_else(|| {
            get_field(record, col_index.serving_size).and_then(|size_str| {
                serving_qty_regex().captures(size_str)
                    .and_then(|c| c.get(1))
                    .and_then(|m| {
                        let cleaned = m.as_str().replace(',', ".");
                        cleaned.parse::<f64>().ok()
                    })
            })
        });

    // Validation: must have energy_kcal AND at least one of (carbohydrates, fat, proteins)
    let energy_kcal = to_num(get_field(record, col_index.energy_kcal_100g));
    let carbohydrates = to_num(get_field(record, col_index.carbohydrates_100g));
    let fat = to_num(get_field(record, col_index.fat_100g));
    let proteins = to_num(get_field(record, col_index.proteins_100g));

    if energy_kcal.is_none() || (carbohydrates.is_none() && fat.is_none() && proteins.is_none()) {
        return Ok(None);
    }

    // Build all nutrient sub-structs
    let macros = MacroNutrients {
        energy_kcal,
        energy_kj: to_num(get_field(record, col_index.energy_kj_100g)),
        carbohydrates,
        fat,
        proteins,
        sugars: to_num(get_field(record, col_index.sugars_100g)),
        fiber: to_num(get_field(record, col_index.fiber_100g)),
        salt: to_num(get_field(record, col_index.salt_100g)),
        added_sugars: to_num(get_field(record, col_index.added_sugars_100g)),
        sucrose: to_num(get_field(record, col_index.sucrose_100g)),
        glucose: to_num(get_field(record, col_index.glucose_100g)),
        fructose: to_num(get_field(record, col_index.fructose_100g)),
        galactose: to_num(get_field(record, col_index.galactose_100g)),
        lactose: to_num(get_field(record, col_index.lactose_100g)),
        maltose: to_num(get_field(record, col_index.maltose_100g)),
        maltodextrins: to_num(get_field(record, col_index.maltodextrins_100g)),
        psicose: to_num(get_field(record, col_index.psicose_100g)),
        starch: to_num(get_field(record, col_index.starch_100g)),
        polyols: to_num(get_field(record, col_index.polyols_100g)),
        erythritol: to_num(get_field(record, col_index.erythritol_100g)),
        isomalt: to_num(get_field(record, col_index.isomalt_100g)),
        maltitol: to_num(get_field(record, col_index.maltitol_100g)),
        sorbitol: to_num(get_field(record, col_index.sorbitol_100g)),
        soluble_fiber: to_num(get_field(record, col_index.soluble_fiber_100g)),
        insoluble_fiber: to_num(get_field(record, col_index.insoluble_fiber_100g)),
        polydextrose: to_num(get_field(record, col_index.polydextrose_100g)),
    };

    let vitamins = Vitamins {
        vitamin_a: to_num(get_field(record, col_index.vitamin_a_100g)),
        beta_carotene: to_num(get_field(record, col_index.beta_carotene_100g)),
        vitamin_d: to_num(get_field(record, col_index.vitamin_d_100g)),
        vitamin_e: to_num(get_field(record, col_index.vitamin_e_100g)),
        vitamin_k: to_num(get_field(record, col_index.vitamin_k_100g)),
        vitamin_c: to_num(get_field(record, col_index.vitamin_c_100g)),
        vitamin_b1: to_num(get_field(record, col_index.vitamin_b1_100g)),
        vitamin_b2: to_num(get_field(record, col_index.vitamin_b2_100g)),
        vitamin_pp: to_num(get_field(record, col_index.vitamin_pp_100g)),
        vitamin_b6: to_num(get_field(record, col_index.vitamin_b6_100g)),
        vitamin_b9: to_num(get_field(record, col_index.vitamin_b9_100g)),
        folates: to_num(get_field(record, col_index.folates_100g)),
        vitamin_b12: to_num(get_field(record, col_index.vitamin_b12_100g)),
        biotin: to_num(get_field(record, col_index.biotin_100g)),
        pantothenic_acid: to_num(get_field(record, col_index.pantothenic_acid_100g)),
        choline: to_num(get_field(record, col_index.choline_100g)),
        phylloquinone: to_num(get_field(record, col_index.phylloquinone_100g)),
        inositol: to_num(get_field(record, col_index.inositol_100g)),
    };

    let minerals = Minerals {
        sodium: to_num(get_field(record, col_index.sodium_100g)),
        calcium: to_num(get_field(record, col_index.calcium_100g)),
        phosphorus: to_num(get_field(record, col_index.phosphorus_100g)),
        iron: to_num(get_field(record, col_index.iron_100g)),
        magnesium: to_num(get_field(record, col_index.magnesium_100g)),
        zinc: to_num(get_field(record, col_index.zinc_100g)),
        copper: to_num(get_field(record, col_index.copper_100g)),
        manganese: to_num(get_field(record, col_index.manganese_100g)),
        fluoride: to_num(get_field(record, col_index.fluoride_100g)),
        selenium: to_num(get_field(record, col_index.selenium_100g)),
        chromium: to_num(get_field(record, col_index.chromium_100g)),
        molybdenum: to_num(get_field(record, col_index.molybdenum_100g)),
        iodine: to_num(get_field(record, col_index.iodine_100g)),
        potassium: to_num(get_field(record, col_index.potassium_100g)),
        chloride: to_num(get_field(record, col_index.chloride_100g)),
        silica: to_num(get_field(record, col_index.silica_100g)),
        bicarbonate: to_num(get_field(record, col_index.bicarbonate_100g)),
        sulphate: to_num(get_field(record, col_index.sulphate_100g)),
        nitrate: to_num(get_field(record, col_index.nitrate_100g)),
    };

    let fats = FatBreakdown {
        saturated: to_num(get_field(record, col_index.saturated_fat_100g)),
        unsaturated: to_num(get_field(record, col_index.unsaturated_fat_100g)),
        monounsaturated: to_num(get_field(record, col_index.monounsaturated_fat_100g)),
        polyunsaturated: to_num(get_field(record, col_index.polyunsaturated_fat_100g)),
        trans: to_num(get_field(record, col_index.trans_fat_100g)),
        cholesterol: to_num(get_field(record, col_index.cholesterol_100g)),
        omega_3: to_num(get_field(record, col_index.omega_3_fat_100g)),
        omega_6: to_num(get_field(record, col_index.omega_6_fat_100g)),
        omega_9: to_num(get_field(record, col_index.omega_9_fat_100g)),
        alpha_linolenic_acid: to_num(get_field(record, col_index.alpha_linolenic_acid_100g)),
        eicosapentaenoic_acid: to_num(get_field(record, col_index.eicosapentaenoic_acid_100g)),
        docosahexaenoic_acid: to_num(get_field(record, col_index.docosahexaenoic_acid_100g)),
        linoleic_acid: to_num(get_field(record, col_index.linoleic_acid_100g)),
        arachidonic_acid: to_num(get_field(record, col_index.arachidonic_acid_100g)),
        gamma_linolenic_acid: to_num(get_field(record, col_index.gamma_linolenic_acid_100g)),
        dihomo_gamma_linolenic_acid: to_num(get_field(record, col_index.dihomo_gamma_linolenic_acid_100g)),
        oleic_acid: to_num(get_field(record, col_index.oleic_acid_100g)),
        elaidic_acid: to_num(get_field(record, col_index.elaidic_acid_100g)),
        gondoic_acid: to_num(get_field(record, col_index.gondoic_acid_100g)),
        mead_acid: to_num(get_field(record, col_index.mead_acid_100g)),
        erucic_acid: to_num(get_field(record, col_index.erucic_acid_100g)),
        nervonic_acid: to_num(get_field(record, col_index.nervonic_acid_100g)),
        butyric_acid: to_num(get_field(record, col_index.butyric_acid_100g)),
        caproic_acid: to_num(get_field(record, col_index.caproic_acid_100g)),
        caprylic_acid: to_num(get_field(record, col_index.caprylic_acid_100g)),
        capric_acid: to_num(get_field(record, col_index.capric_acid_100g)),
        lauric_acid: to_num(get_field(record, col_index.lauric_acid_100g)),
        myristic_acid: to_num(get_field(record, col_index.myristic_acid_100g)),
        palmitic_acid: to_num(get_field(record, col_index.palmitic_acid_100g)),
        stearic_acid: to_num(get_field(record, col_index.stearic_acid_100g)),
        arachidic_acid: to_num(get_field(record, col_index.arachidic_acid_100g)),
        behenic_acid: to_num(get_field(record, col_index.behenic_acid_100g)),
        lignoceric_acid: to_num(get_field(record, col_index.lignoceric_acid_100g)),
        cerotic_acid: to_num(get_field(record, col_index.cerotic_acid_100g)),
        montanic_acid: to_num(get_field(record, col_index.montanic_acid_100g)),
        melissic_acid: to_num(get_field(record, col_index.melissic_acid_100g)),
    };

    let other = OtherNutrients {
        caffeine: to_num(get_field(record, col_index.caffeine_100g)),
        taurine: to_num(get_field(record, col_index.taurine_100g)),
        carnitine: to_num(get_field(record, col_index.carnitine_100g)),
        beta_glucan: to_num(get_field(record, col_index.beta_glucan_100g)),
        alcohol: to_num(get_field(record, col_index.alcohol_100g)),
        nucleotides: to_num(get_field(record, col_index.nucleotides_100g)),
        casein: to_num(get_field(record, col_index.casein_100g)),
        serum_proteins: to_num(get_field(record, col_index.serum_proteins_100g)),
        methylsulfonylmethane: to_num(get_field(record, col_index.methylsulfonylmethane_100g)),
        energy_from_fat: to_num(get_field(record, col_index.energy_from_fat_100g)),
        added_salt: to_num(get_field(record, col_index.added_salt_100g)),
    };

    let breakdown = Breakdown {
        macros,
        vitamins,
        minerals,
        fats,
        other,
    };

    let product = Product {
        code: code.clone(),
        product_name: name.clone(),
        generic_name,
        ingredients_text,
        brands: brand.clone(),
        main_category: main_category.clone(),
        serving_size,
        serving_unit: serving_unit.clone(),
        breakdown,
    };

    write_product_file(&product, &code)?;

    // Catalog entry uses per_100g macros with 100g as serving
    let catalog_serving_size = serving_size.or(Some(100.0));
    let catalog_serving_unit = serving_unit.clone().or_else(|| Some("g".to_string()));

    let mut catalog_entries = Vec::new();
    for country_code in &country_codes {
        let catalog_entry = CatalogEntry {
            code: code.clone(),
            name: name.clone(),
            brand: brand.clone(),
            country: Some(country_code.clone()),
            serving_size: catalog_serving_size,
            serving_unit: catalog_serving_unit.clone(),
            fiber: product.breakdown.macros.fiber,
            carbs: product.breakdown.macros.carbohydrates,
            fat: product.breakdown.macros.fat,
            protein: product.breakdown.macros.proteins,
        };
        catalog_entries.push((catalog_entry, country_code.clone()));
    }

    Ok(Some((catalog_entries, brand)))
}
