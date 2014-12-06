-- Report number of parcels by county.
SELECT   county_id, count(*)
FROM     parcels
GROUP BY county_id
ORDER BY county_id;

-- Report sum of building_sqft by county.
SELECT   county_id, sum(building_sqft) AS building_sqft,
FROM     parcels
GROUP BY county_id
ORDER BY county_id;

-- Report sum of non_residential_sqft by county.
SELECT   county_id, sum(non_residential_sqft) AS non_residential_sqft,
FROM     parcels
GROUP BY county_id
ORDER BY county_id;


-- Report sum of residential_units by county.
SELECT   county_id, sum(residential_units) AS residential_units,
FROM     parcels
GROUP BY county_id
ORDER BY county_id;
