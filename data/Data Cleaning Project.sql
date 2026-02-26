/* 

Cleaning Data in SQL Queries Demonstration

*/



SELECT *
FROM PortfolioProject..NashvilleHousing

------------------------------------------------------------------
-- Standardize Date Format

SELECT SaleDateConverted, CONVERT(Date, SaleDate)
FROM PortfolioProject..NashvilleHousing

--Update NashvilleHousing
--SET SaleDate = CONVERT(Date, SaleDate)
-- This for some reason did not convert the SaleDate column to the cleaner format

ALTER TABLE NashvilleHousing
ADD SaleDateConverted Date; 

Update NashvilleHousing
SET SaleDateConverted = CONVERT(Date, SaleDate)


------------------------------------------------------------------
-- Populate null Property Address Data

SELECT *
FROM PortfolioProject..NashvilleHousing
--Where PropertyAddress is null 
ORDER BY ParcelID

SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM PortfolioProject..NashvilleHousing a
JOIN PortfolioProject..NashvilleHousing b
	ON a.ParcelID = b.ParcelID 
	AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress is null

Update a
SET PropertyAddress = ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM PortfolioProject..NashvilleHousing a
JOIN PortfolioProject..NashvilleHousing b
	ON a.ParcelID = b.ParcelID 
	AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.propertyAddress is null

------------------------------------------------------------------
-- Breaking Address into Individual Columns (Address, City, State) 

SELECT PropertyAddress 
FROM PortfolioProject..NashvilleHousing

SELECT 
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1 ) as Address
, SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress)+1, LEN(PropertyAddress)) as Address
FROM PortfolioProject..NashvilleHousing


ALTER TABLE NashvilleHousing
ADD PropertySplitAddress Nvarchar(255); 

Update NashvilleHousing
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1 ) 

ALTER TABLE NashvilleHousing
Add PropertySplitCity Nvarchar(255); 

Update NashvilleHousing
SET PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress)+1, LEN(PropertyAddress)) 

SELECT *
FROM PortfolioProject..NashvilleHousing

-- Property Address has been fixed, now to fix OwnerAddress 

SELECT OwnerAddress
FROM PortfolioProject..NashvilleHousing

SELECT
PARSENAME(REPLACE(OwnerAddress,',' , '.'), 3)
,PARSENAME(REPLACE(OwnerAddress,',' , '.'), 2)
,PARSENAME(REPLACE(OwnerAddress,',' , '.'), 1)
FROM PortfolioProject..NashvilleHousing

ALTER TABLE NashvilleHousing
ADD OwnerSplitAddress Nvarchar(255); 

Update NashvilleHousing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress,',' , '.'), 3)

ALTER TABLE NashvilleHousing
ADD OwnerSplitCity Nvarchar(255); 

Update NashvilleHousing
SET OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress,',' , '.'), 2)

ALTER TABLE NashvilleHousing
ADD OwnerSplitState Nvarchar(255); 

Update NashvilleHousing
SET OwnerSplitState = PARSENAME(REPLACE(OwnerAddress,',' , '.'), 1)

------------------------------------------------------------------
-- Changing Y and N to Yes and No in "Sold as Vacant" field

SELECT DISTINCT(SoldasVacant), COUNT(SoldAsVacant)
FROM PortfolioProject..NashvilleHousing
GROUP BY SoldAsVacant
ORDER BY 2 

SELECT SoldAsVacant
, CASE When SoldAsVacant = 'Y' THEN 'Yes'
	   WHEN SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END
FROM PortfolioProject..NashvilleHousing

Update NashvilleHousing
SET SoldAsVacant = CASE When SoldAsVacant = 'Y' THEN 'Yes'
	   WHEN SoldAsVacant = 'N' THEN 'No'
	   ELSE SoldAsVacant
	   END

-------------------------------------------------------------------
-- Remove Duplicates

WITH RowNumCTE AS (
SELECT *, 
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID,
				 PropertyAddress,
				 SalePrice,
				 SaleDate,
				 LegalReference
				 ORDER BY 
				    UniqueID
					) row_num
	
FROM PortfolioProject..NashvilleHousing
--ORDER BY ParcelID
)
--SELECT * 
--FROM RowNumCTE
--Where row_num > 1
--ORDER By PropertyAddress
-- Used this select statement to query what I will delete

DELETE  
FROM RowNumCTE
WHERE row_num > 1


-------------------------------------------------------------------------
-- Delete Unused Columns 

SELECT * 
FROM PortfolioProject..NashvilleHousing

ALTER TABLE PortfolioProject..NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress, SaleDate
