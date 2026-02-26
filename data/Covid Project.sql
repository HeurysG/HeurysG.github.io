SELECT * 
FROM [Portfolio Project]..CovidDeaths
Order by 3,4 

--SELECT * 
--FROM [Portfolio Project]..CovidVaccinations
--Order by 3,4

-- Looking at all data that will be used 

SELECT Location, Date, Total_Cases, New_cases, Total_Deaths, Population 
FROM [Portfolio Project]..CovidDeaths
ORDER BY 1,2 

-- Looking at Total Cases vs Total Deaths in US
-- Estimate of mortality rate

SELECT Continent, Location, Date, Total_Cases, Total_Deaths, (total_deaths/total_cases)* 100 as MortalityRate
FROM [Portfolio Project]..CovidDeaths
WHERE Location like '%states%' AND continent IS NOT null
ORDER BY 1,2

-- Looking at Total Cases vs Population in US 
-- Shows what percentage of population contracted COVID-19

SELECT Continent, Location, Date, Total_Cases, Population, (total_cases/population)* 100 as Prevalence
FROM [Portfolio Project]..CovidDeaths
WHERE Location LIKE '%states%' AND continent IS NOT null
ORDER BY 1,2 

-- Looking at Countries with Highest Infection Rate compared to Population

SELECT Continent, Location,Population, MAX(total_cases) as HighestInfectionCount , Max((total_cases/population))* 100 as Prevalence
FROM [Portfolio Project]..CovidDeaths
--Where Location like '%states%'
WHERE continent IS NOT null
GROUP BY population, location, continent
ORDER BY Prevalence DESC 

-- Showing Countries with Highest Death Count per Population 

SELECT Location, MAX(cast(total_deaths as int)) as TotalDeathCount
FROM [Portfolio Project]..CovidDeaths
WHERE continent IS NOT null
--Where Location like '%states%'
GROUP BY Location
ORDER BY TotalDeathCount DESC 

-- Showing continents with the highest death count per population 

SELECT continent, MAX(cast(total_deaths as int)) as TotalDeathCount
FROM [Portfolio Project]..CovidDeaths
WHERE continent IS NOT null
--Where Location like '%states%'
GROUP BY continent
ORDER BY TotalDeathCount DESC 

-- Global Numbers

SELECT SUM(new_cases) as TotalCases , Sum(cast(new_deaths as int)) as TotalDeaths , SUM(cast(new_deaths as int))/SUM(new_cases)* 100 as DeathPercentage
FROM [Portfolio Project]..CovidDeaths
-- Where Location like '%states%' 
WHERE continent IS NOT null
-- GROUP BY Date
ORDER BY 1,2

-- Looking at Total Population vs Vaccinations

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location Order By dea.location, 
dea.date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population) * 100
FROM [Portfolio Project]..CovidDeaths dea
JOIN [Portfolio Project]..CovidVaccinations vac
	ON dea.location = vac.location 
	AND dea.date = vac.date 
WHERE dea.Continent IS NOT null 
ORDER BY 2,3

-- CTE 

----With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated) 
-- as 
-- (
----SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
--, SUM(CONVERT(bigint, vac.new_vaccinations)) OVER (Partition by dea.location Order By dea.location, 
--dea.date) as RollingPeopleVaccinated
----, (RollingPeopleVaccinated/population) * 100
--FROM [Portfolio Project]..CovidDeaths dea
--JOIN [Portfolio Project]..CovidVaccinations vac
-- ON dea.location = vac.location 
-- and dea.date = vac.date 
-- WHERE dea.Continent is not null 
-- -- ORDER BY 2,3
-- ) 

-- --SELECT *, (RollingPeopleVaccinated/Population) * 100 
-- --FROM PopvsVac
 
-- Temp Table 

DROP Table if exists #PercentPopulationVaccinated
CREATE Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255), 
Date datetime, 
Population numeric,
New_vaccinations numeric, 
RollingPeopleVaccinated numeric
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.Continent, dea.Location, dea.Date, dea.Population, vac.New_Vaccinations
, SUM(Convert(bigint, vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location,
 dea.Date) as RollingPeopleVaccinated 

 --, (RollingPeopleVaccinated/population) * 100
 From [Portfolio Project]..CovidDeaths dea 
 JOIN [Portfolio Project]..CovidVaccinations vac 
	ON dea.location = vac.location 
	AND dea.date = vac.date 
-- where dea.continent is not null
-- order by 2,3 

SELECT *, (RollingPeopleVaccinated/Population) * 100 
FROM #PercentPopulationVaccinated

-- Creating View to store data for later visualizations 

Create View PercentPopulationVaccinated as
SELECT dea.Continent, dea.Location, dea.Date, dea.Population, vac.New_Vaccinations
, SUM(Convert(bigint, vac.new_vaccinations)) OVER (Partition by dea.location Order by dea.location,
 dea.Date) as RollingPeopleVaccinated 
 --, (RollingPeopleVaccinated/population) * 100
 FROM [Portfolio Project]..CovidDeaths dea 
 JOIN [Portfolio Project]..CovidVaccinations vac 
	ON dea.location = vac.location 
	AND dea.date = vac.date 
WHERE dea.continent IS NOT null
-- order by 2,3 
