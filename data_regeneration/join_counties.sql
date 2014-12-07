DROP TABLE IF EXISTS parcels;
CREATE TABLE parcels (
  id serial PRIMARY KEY,
  county_id char(3) NOT NULL,
  apn text NOT NULL,
  parcel_id_local text,
  land_use_type_id text,
  res_type text,
  land_value float,
  improvement_value float,
  year_assessed float,
  year_built float,
  building_sqft float,
  non_residential_sqft float,
  residential_units float,
  sqft_per_unit float,
  stories float,
  tax_exempt integer,
  geom geometry(MultiPolygon, 2768) NOT NULL
);

INSERT INTO parcels (
  county_id, apn, parcel_id_local, land_use_type_id,
  res_type, land_value, improvement_value, year_assessed,
  year_built, building_sqft, non_residential_sqft,
  residential_units, sqft_per_unit, stories, tax_exempt,
  geom
)
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_ala as a,
         (SELECT   apn_sort, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_ala
          GROUP BY apn_sort) AS p
  WHERE  a.apn = p.apn_sort
  UNION
  SELECT a.county_id, a.apn::int::text, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_cnc as a,
         (SELECT   parc_py_id,
                   ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_cnc_poly
          GROUP BY parc_py_id) AS p
  WHERE  a.apn = p.parc_py_id
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_mar as a,
         (SELECT   parcel, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_mar
          GROUP BY parcel) AS p
  WHERE  a.apn = p.parcel
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_nap as a,
         (SELECT   asmt, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_nap
          GROUP BY asmt) AS p
  WHERE  a.apn = p.asmt
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_scl as a,
         (SELECT   parcel, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_scl
          GROUP BY parcel) AS p
  WHERE  a.apn = p.parcel
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_sol as a,
         (SELECT   apn, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_sol
          GROUP BY apn) AS p
  WHERE  a.apn = p.apn
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_son as a,
         (SELECT   apn, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_son
          GROUP BY apn) AS p
  WHERE  a.apn = p.apn
         AND a.apn SIMILAR TO '%[0-9]{3}' OR a.apn LIKE '%COM'
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_sfr as a,
         (SELECT   blklot, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_sfr
          GROUP BY blklot) AS p
  WHERE  a.apn = p.blklot
  UNION
  SELECT a.county_id, a.apn, a.parcel_id_local, a.land_use_type_id,
         a.res_type, a.land_value, a.improvement_value, a.year_assessed,
         a.year_built, a.building_sqft, a.non_residential_sqft,
         a.residential_units, a.sqft_per_unit, a.stories, a.tax_exempt, p.geom
  FROM   staging.attributes_smt as a,
         (SELECT   apn, ST_CollectionExtract(ST_Collect(geom), 3) AS geom
          FROM     staging.parcels_smt
          GROUP BY apn) AS p
  WHERE  a.apn = p.apn;

ALTER TABLE parcels ADD CONSTRAINT parcels_apn_unique
  UNIQUE (county_id, apn);

CREATE INDEX parcels_geom_gist ON parcels
  USING gist (geom);
