SELECT * FROM 
`portfolioprojects-377116.Covid_data.CovidVaccinations`
order by 1,2

--Selecting  Total death vs Total cases
--There is less likely chance of dying if you got covid in Nepal

SELECT location, date, total_cases, total_deaths, population, (total_deaths/total_cases)*100 as death_percentage
from  `portfolioprojects-377116.Covid_data.CovidDeaths` 
where location = "Nepal"
order by 1,2



-- Total Infection rate

SELECT location, date, total_cases, population, (total_cases/population)*100 as infected_percentage
from  `portfolioprojects-377116.Covid_data.CovidDeaths` 
order by 1,2

--Shows what percentage of population got covid
SELECT location, population, max(total_cases) as Heighest_infection_count, MAX(total_cases/population) * 100 as Percent_Population_Infected
FROM `portfolioprojects-377116.Covid_data.CovidDeaths` 
WHERE continent is not null
and location not in ('World', 'European Union', 'International')
GROUP BY location, population
order by Percent_Population_Infected desc

-- Shows what percentage of population got covid with date 

SELECT location, population, date, max(total_cases) as Heighest_infection_count, MAX(total_cases/population) * 100 as Percent_Population_Infected
FROM `portfolioprojects-377116.Covid_data.CovidDeaths` 
WHERE continent is not null
--and location not in ('World', 'European Union', 'International')
GROUP BY location, population,date
order by Percent_Population_Infected desc
limit 20


--Top 10 most infected countries globally from 2019-2022

SELECT location,continent, population, MAX(total_cases) as Total_infection_count, 
       MAX(total_cases/population) * 100 as Percent_population_infected, max(total_deaths) AS TotalDeathCount, (max(total_deaths)/MAX(total_cases))*100 as Death_percentage
FROM `portfolioprojects-377116.Covid_data.CovidDeaths` 
WHERE continent IS NOT NULL
GROUP BY continent,location,population
ORDER BY TotalDeathCount DESC
LIMIT 10




--Countires with highest death count per population

SELECT location, max(total_deaths) AS TotalDeathCount
from  `portfolioprojects-377116.Covid_data.CovidDeaths` 
where continent is not null
group by location
order by TotalDeathCount desc


--Break down by continent
--continent with heighst death count per population

SELECT continent, max(total_deaths) AS TotalDeathCount
from  `portfolioprojects-377116.Covid_data.CovidDeaths` 
where continent is not null
group by continent
order by TotalDeathCount desc

--Total number of cases and deaths across the globe

SELECT  sum(new_cases) as total_cases,sum(new_deaths) as total_deaths, (sum(new_deaths)/sum(new_cases))*100 as death_percentage
from  `portfolioprojects-377116.Covid_data.CovidDeaths` 
where continent is not null
--group by date
order by 1,2


-- joining the two tables
-- Total population vs vaccinations
-- Applying rolling count using partition by function

select death.continent, death.location, death.date,population, vacc.new_vaccinations,
sum(vacc.new_vaccinations) over (partition by death.location order by death.location, death.date) as Rolling_people_vaccinated 
-- partition by location becoz if done by continent than the number will be off charts and the rolling will not keep on going
-- so from every location it gets reset and counts again based on location so at the end of location we will get the total vacc
from `portfolioprojects-377116.Covid_data.CovidDeaths` death
join `portfolioprojects-377116.Covid_data.CovidVaccinations` vacc
on death.location = vacc.location
and death.date = vacc.date
where death.continent is not null
order by 2,3


--Total cases, total vaccination, total death and death rate by location

SELECT DISTINCT death.location,death.population, SUM(death.total_cases) AS total_cases, SUM(vacc.people_vaccinated) AS total_vaccinations, sum(death.total_deaths) as Total_death,
  CASE WHEN SUM(vacc.people_vaccinated) > 0 THEN (SUM(death.total_deaths) / SUM(vacc.people_vaccinated)) END AS death_rate
FROM `portfolioprojects-377116.Covid_data.CovidDeaths` death
JOIN `portfolioprojects-377116.Covid_data.CovidVaccinations` vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL AND vacc.people_fully_vaccinated > 0
GROUP BY death.location, death.population
order by 5 desc




-- Now to look at the total pop vs vacc we will be using Rolling_people_vaccinated column becoz at the bottom will be the max number
-- Total population vs vaccinations
-- now we cannot use the table used to do calculations so we either need to make CTE or Temp table

-- Using CTE(common table expression)

WITH popvsvacc AS
(
  SELECT death.continent, death.location, death.date, death.population, vacc.new_vaccinations,
  SUM(vacc.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as Rolling_people_vaccinated 
  FROM `portfolioprojects-377116.Covid_data.CovidDeaths` death
  JOIN `portfolioprojects-377116.Covid_data.CovidVaccinations` vacc
    ON death.location = vacc.location
    AND death.date = vacc.date
  WHERE death.continent IS NOT NULL
)
SELECT *, (Rolling_people_vaccinated/population)*100 as Vaccinated_percentage_total
FROM popvsvacc

-- Creating view to store data for later visualization

CREATE VIEW portfolioprojects-377116.Covid_data.PercentPopulationVacccinated AS 
SELECT death.continent, death.location, death.date, death.population, vacc.new_vaccinations,
  SUM(vacc.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as Rolling_people_vaccinated 
FROM `portfolioprojects-377116.Covid_data.CovidDeaths` death
JOIN `portfolioprojects-377116.Covid_data.CovidVaccinations` vacc
    ON death.location = vacc.location
    AND death.date = vacc.date
WHERE death.continent IS NOT NULL


-- Getting maximum number of vaccinated people out of total population and their percentage with CTE 

WITH popvsvacc AS
(
  SELECT death.continent, death.location, death.date, death.population, vacc.new_vaccinations,
  SUM(vacc.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as Rolling_people_vaccinated 
  FROM `portfolioprojects-377116.Covid_data.CovidDeaths` death
  JOIN `portfolioprojects-377116.Covid_data.CovidVaccinations` vacc
    ON death.location = vacc.location
    AND death.date = vacc.date
  WHERE death.continent IS NOT NULL
)
SELECT Location, Population, MAX(Rolling_people_vaccinated) as Maximum_Rolling_people_vaccinated,
MAX((Rolling_people_vaccinated/population)*100) as Maximum_Vaccinated_percentage_total
FROM popvsvacc
WHERE location = 'Nepal'
GROUP BY Location, population



-- Temp Table

DROP TABLE IF EXISTS PercentagePopulationVaccinated;
CREATE temp TABLE PercentagePopulationVaccinated
(
  Continent nvarchar(255),
  Location nvarchar(255),
  Population numeric,
  Date datetime,
  New_vaccinations numeric,
  RollingPeopleVaccinated numeric
);

INSERT INTO PercentagePopulationVaccinated
SELECT death.continent, death.location, death.date, death.population, vacc.new_vaccinations,
SUM(vacc.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as Rolling_people_vaccinated
FROM portfolioprojects-377116.Covid_data.CovidDeaths death
JOIN portfolioprojects-377116.Covid_data.CovidVaccinations vacc
ON death.location = vacc.location
AND death.date = vacc.date
WHERE death.continent IS NOT NULL;

SELECT *, (Rolling_people_vaccinated/population)*100 as Vaccinated_percentage_total
FROM PercentagePopulationVaccinated;



