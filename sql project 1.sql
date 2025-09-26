-- DATA CLEANSING CARRIED OUT IN 4 GENERAL STEPS
-- 1. Remove Duplicates
-- 2. Standardize data
-- 3. Filling Null and Blank values
-- 4. Remove unnecessary columns

-- create additional table where we can update/ delete and manipulate data. Table schmea should replicate the original table; Raw data should not be affected. 
CREATE TABLE layoff_staging
LIKE layoffs;

-- populate the new table with raw data
INSERT layoff_staging
SELECT * FROM layoffs;

-- 1.REMOVE DUPLICATES
-- identify any redudancy/ duplicacy in data using row number () function
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
							stage, funds_raised_millions) AS row_num
FROM layoff_staging;

WITH duplicate_cte AS
(SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoff_staging)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

-- create another table with row number build into the schmea to help identify duplicates easily. 
CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_staging2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_staging;

-- Disable SQL_SAFE_UPDATES TO SUCCESFULLY UPDATE OR ELEMINATE ROWS & COLUMNS
SET SQL_SAFE_UPDATES = 0;
DELETE
FROM layoff_staging2
WHERE row_num >1;

-- 2.STANDARDIZING DATA
-- remove white spaces 
UPDATE layoff_staging2
SET company=TRIM(company);

-- standardize name of industry like 'Crypto', 'Crypto currency' under a single identifier
UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- remove any trailing ',' or '.' from any attributes
UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Change data type and date format of date column
UPDATE layoff_staging2
SET `date`= str_to_date(`date`, '%m/%d/%Y');
ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

-- 3.NULL AND BLANKS VALUES
-- identify values represnted as NULL or empty ''.
SELECT *
FROM layoff_staging2
WHERE industry IS NULL OR industry ='';

-- UPDATE all missing/blank values as NULL, easier to update NULL values.
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';

-- Identify all companies which have missing industry in one instance but present in other instances.
SELECT *
FROM layoff_staging2 t1
JOIN layoff_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Update all missing values in industry attribute by filling values from other instances of same company 
UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- 4.REMOVE UNNECESSARY COLUMNS. 
-- elimnate redundant data that doesn't add any value to this analysis.
DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;
 
-- Remove row_num column as we have already elimninated duplicate data
ALTER TABLE layoff_staging2
DROP row_num;
