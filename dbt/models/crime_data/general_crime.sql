{{config(materialized = 'view',

    partition_by = {
      "field": "crime_date",
      "data_type": "timestamp",
      "granularity": "month"
})}}

SELECT
    id,
    case_number,
    crime_date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest, 
    domestic, 
    beat_incident,
    crime_district,
    crime_ward ,
    community_area,
    fbi_code,  
    x_coor, 
    y_coor,
    year,
    upload_date,
    latitude,  
    longitude,
    location, 


from {{ref('crime_stage')}}{{config(materialized = 'view',

    partition_by = {
      "field": "crime_date",
      "data_type": "timestamp",
      "granularity": "month"
})}}

SELECT
    id,
    case_number,
    crime_date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest, 
    domestic, 
    beat_incident,
    crime_district,
    crime_ward ,
    community_area,
    fbi_code,  
    x_coor, 
    y_coor,
    year,
    upload_date,
    latitude,  
    longitude,
    location, 


from {{ref('crime_stage')}}