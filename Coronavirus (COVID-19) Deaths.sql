-- Databricks notebook source
-- DBTITLE 1,Introduction
-- MAGIC %md
-- MAGIC # COVID-19 Data Exploration (Early Years)
-- MAGIC
-- MAGIC This notebook explores COVID-19 death and infection data across countries during the pandemic's early years.
-- MAGIC
-- MAGIC **Techniques Used:** Aggregate Functions, Window Functions, CTEs, Temp Tables, Joins, Views, Data Type Conversions
-- MAGIC
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- DBTITLE 1,Raw Data Preview
SELECT *
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
ORDER BY 3, 4

-- COMMAND ----------

-- DBTITLE 1,Key Metrics Section
-- MAGIC %md
-- MAGIC ## Key Metrics Overview
-- MAGIC Selecting the core columns we'll use throughout this analysis.

-- COMMAND ----------

-- DBTITLE 1,Core Data Selection
SELECT
  location,
  date,
  total_cases,
  new_cases,
  total_deaths,
  population
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Death Rate Section
-- MAGIC %md
-- MAGIC ## Death Rate Analysis (United States)
-- MAGIC Calculating the likelihood of dying after contracting COVID-19 — expressed as `(total_deaths / total_cases) * 100`.

-- COMMAND ----------

-- DBTITLE 1,Death Percentage Over Time
SELECT
  location,
  date,
  total_cases,
  total_deaths,
  ROUND((total_deaths / total_cases) * 100, 2) AS death_percentage
FROM users.w196717.covid_deaths
WHERE location ILIKE '%states%'
  AND continent IS NOT NULL
ORDER BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Infection Rate Section
-- MAGIC %md
-- MAGIC ## Infection Rate vs Population (United States)
-- MAGIC What percentage of the U.S. population has been infected over time?

-- COMMAND ----------

-- DBTITLE 1,Percent Population Infected Over Time
SELECT
  location,
  date,
  population,
  total_cases,
  ROUND((total_cases / population) * 100, 4) AS pct_population_infected
FROM users.w196717.covid_deaths
WHERE location ILIKE '%states%'
ORDER BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Global Comparison Section
-- MAGIC %md
-- MAGIC ## Global Comparison: Highest Infection Rates
-- MAGIC Ranking all countries by their peak infection rate relative to population size.

-- COMMAND ----------

-- DBTITLE 1,Countries Ranked by Infection Rate
SELECT
  location,
  population,
  MAX(total_cases)                        AS highest_infection_count,
  ROUND(MAX(total_cases / population) * 100, 2) AS pct_population_infected
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY pct_population_infected DESC

-- COMMAND ----------

-- DBTITLE 1,Death Count Section
-- MAGIC %md
-- MAGIC ## Death Count Rankings
-- MAGIC Identifying countries and continents with the highest total death counts.

-- COMMAND ----------

-- DBTITLE 1,Countries by Total Death Count
SELECT
  location,
  MAX(CAST(total_deaths AS INT)) AS total_death_count
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count DESC

-- COMMAND ----------

-- DBTITLE 1,Continents by Total Death Count
SELECT
  continent,
  MAX(CAST(total_deaths AS INT)) AS total_death_count
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY total_death_count DESC

-- COMMAND ----------

-- DBTITLE 1,Global Numbers Section
-- MAGIC %md
-- MAGIC ## Global Totals
-- MAGIC Aggregate worldwide case and death figures with overall death percentage.

-- COMMAND ----------

-- DBTITLE 1,Global Case and Death Totals
SELECT
  SUM(new_cases)                                          AS total_cases,
  SUM(CAST(new_deaths AS INT))                            AS total_deaths,
  ROUND(SUM(CAST(new_deaths AS INT)) / SUM(new_cases) * 100, 2) AS death_percentage
FROM users.w196717.covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Vaccination Section
-- MAGIC %md
-- MAGIC ## Vaccination Progress
-- MAGIC Joining deaths and vaccinations data to track rolling vaccination counts and percentage of population vaccinated.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- DBTITLE 1,Rolling Vaccinations by Country
SELECT
  dea.continent,
  dea.location,
  dea.date,
  dea.population,
  vac.new_vaccinations,
  SUM(CAST(vac.new_vaccinations AS INT)) OVER (
    PARTITION BY dea.location
    ORDER BY dea.date
  ) AS rolling_people_vaccinated
FROM users.w196717.covid_deaths dea
JOIN users.w196717.covid_vaccinations vac
  ON dea.location = vac.location
  AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2, 3

-- COMMAND ----------

-- DBTITLE 1,Vaccination % via CTE
WITH PopvsVac AS (
  SELECT
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    vac.new_vaccinations,
    SUM(CAST(vac.new_vaccinations AS INT)) OVER (
      PARTITION BY dea.location
      ORDER BY dea.date
    ) AS rolling_people_vaccinated
  FROM users.w196717.covid_deaths dea
  JOIN users.w196717.covid_vaccinations vac
    ON dea.location = vac.location
    AND dea.date = vac.date
  WHERE dea.continent IS NOT NULL
)
SELECT
  *,
  ROUND((rolling_people_vaccinated / population) * 100, 2) AS pct_vaccinated
FROM PopvsVac

-- COMMAND ----------

-- DBTITLE 1,Vaccination % via Temp View
CREATE OR REPLACE TEMP VIEW PercentPopulationVaccinated AS
SELECT
  dea.continent,
  dea.location,
  dea.date,
  dea.population,
  vac.new_vaccinations,
  SUM(CAST(vac.new_vaccinations AS INT)) OVER (
    PARTITION BY dea.location
    ORDER BY dea.date
  ) AS rolling_people_vaccinated
FROM users.w196717.covid_deaths dea
JOIN users.w196717.covid_vaccinations vac
  ON dea.location = vac.location
  AND dea.date = vac.date;

SELECT
  *,
  ROUND((rolling_people_vaccinated / population) * 100, 2) AS pct_vaccinated
FROM PercentPopulationVaccinated