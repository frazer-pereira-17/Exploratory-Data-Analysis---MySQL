-- Data Cleaning

Select *
FROM layoffs;

-- 1. Remove Dupilcates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens.

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging
Select *
From layoffs;

Select *
From layoffs_staging;

-- 1. Remove Duplicates

# First let's check for duplicates
SELECT *
FROM layoffs_staging;

SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM 
		layoffs_staging;
        
        
With duplicate_cte AS
(
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM 
		layoffs_staging
)
Select *
From duplicate_cte
Where row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


CREATE TABLE layoffs_staging2 (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` INT DEFAULT NULL,
`row_num` INT
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, `date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;


Delete
FROM layoffs_staging2
Where row_num > 1;

SELECT *
FROM layoffs_staging2;


-- 2. Standardize Data

Select distinct(trim(company))
From layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

Select *
From layoffs_staging2
where industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
Where industry like 'Crypto%';

SELECT *
FROM layoffs_staging2;

Select distinct country, TRIM(Trailing '.' From country)
From layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = TRIM(Trailing '.' From country)
Where country like 'United States%';

SELECT *
FROM layoffs_staging2;

-- Let's also fix the date columns:
SELECT *
FROM layoffs_staging2;

	
-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;


-- 3. Look at Null Values

Select *
From layoffs_staging2
Where total_laid_off IS NULL;

Select distinct(industry)
From layoffs_staging2
Where industry IS NULL;

Select *
From layoffs_staging2
Where company = 'Airbnb';

-- important

Update layoffs_staging2
Set industry = NULL
Where industry = '';

Select *
From layoffs_staging2 t1                                     
Join layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
where (t1.industry IS NULL OR t1.industry = '')
And t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
Where t1.industry IS NULL
And t2.industry IS NOT NULL;

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase


-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Validate the data again

SELECT * 
FROM layoffs_staging2;




























