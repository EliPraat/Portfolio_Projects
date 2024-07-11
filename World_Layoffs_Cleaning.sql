SELECT * FROM world_layoffs.layoffs;

# Data Cleaning

# 1. Remove Duplicates
# 2. Standardize the Data
# 3. Null and blank values
# 4. Remove any Columns

# Creating new table as to not alter original dataset 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging
;

INSERT layoffs_staging
SELECT *
FROM layoffs;


# Identifying Duplicates using a window function

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num 
FROM layoffs_staging
;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;

# Adding row_num column 
CREATE TABLE `layoffs_staging1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging1
;

INSERT INTO layoffs_staging1
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

SELECT *
FROM layoffs_staging1
WHERE row_num > 1;

# Deleting Duplicates
DELETE
FROM layoffs_staging1
WHERE row_num > 1;

SELECT *
FROM layoffs_staging1;


# Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging1;

UPDATE layoffs_staging1
SET company = TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging1
ORDER BY 1;

SELECT DISTINCT *
FROM layoffs_staging1
WHERE industry LIKE '%Crypto%';

UPDATE layoffs_staging1
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging1
ORDER BY 1;

UPDATE layoffs_staging1
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging1;

            
UPDATE layoffs_staging1
SET `date` = 
    CASE
        WHEN `date` IS NOT NULL AND `date` != 'NULL' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
        ELSE NULL  
    END;


ALTER TABLE layoffs_staging1
MODIFY COLUMN `date` DATE;

# NUll and blank values

SELECT *
FROM layoffs_staging1
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

UPDATE layoffs_staging1
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging1
WHERE industry = 'NULL'
OR industry = ''
;

SELECT *
FROM layoffs_staging1
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging1 t1
JOIN layoffs_staging1 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;
    
UPDATE layoffs_staging1 t1
JOIN layoffs_staging1 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging1;

# Remove any Rows or Columns

# Deleting rows that have NULL values in both the 'total laid off' column as well as the 'percentage laid off' column, 
# as I'm not entirely sure these companies did have layoffs, it's dated saying they did but there is no data to show,
# therefore I can't trust the data.  

SELECT *
FROM layoffs_staging1
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

DELETE
FROM layoffs_staging1
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';


# Deleting row_num column as we no longer need it.
ALTER TABLE layoffs_staging1
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging1;






