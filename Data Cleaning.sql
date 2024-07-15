SELECT * 
FROM world_layoffs.layoffs;



--  Creating a staging table to work in and clean the data and keep the raw data intact in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- the following steps were followed to clean the data
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Looking at null values
-- 4. remove any columns and rows that are not necessary



-- 1. Removing Duplicates


SELECT *
FROM world_layoffs.layoffs_staging
;



SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete  



WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;





-- 2. Standardizing Data

SELECT * 
FROM world_layoffs.layoffs_staging;

-- looking at industry columns with null and empty rows


SELECT *
FROM world_layoffs.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Now looking at these industries and which company they belong to

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company LIKE 'airbnb%';

-- airbnb is a travel, but this one just isn't populated.

-- Setting the blanks to nulls since they easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging t1
JOIN layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM world_layoffs.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- Standardizing the industry column   
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging
ORDER BY industry;

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- looking at the table to see the changes:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging;

-- we have some "United States" and some "United States." with a period at the end. So standardizing this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging
ORDER BY country;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);


SELECT DISTINCT country
FROM world_layoffs.layoffs_staging
ORDER BY country;


-- fixing the date columns since their data type is wrong:
SELECT *
FROM world_layoffs.layoffs_staging;


UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging;







-- 4. remove any columns and rows that are needed 



DELETE FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging;

































