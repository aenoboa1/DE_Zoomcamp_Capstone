{{config(materialized = 'view',

    partition_by = {
      "field": "crime_date",
      "data_type": "timestamp",
      "granularity": "month"
})}}

SELECT
    id,
    case_number,
    cast(date as timestamp) as crime_date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest, 
    domestic, 
    cast(beat as numeric) as beat_incident,
    cast(district as numeric) as crime_district,
    cast(ward as numeric) as crime_ward ,
    community_area,
    fbi_code,  
    cast(x_coordinate as numeric) as x_coor, 
    cast(y_coordinate as numeric) as y_coor,
    year,
    cast(updated_on as timestamp) as upload_date,
    cast(latitude as numeric) as latitude,  
    cast(longitude as numeric) as longitude,
    location, 


from {{source('staging','crimes_data_all_external_table')}}