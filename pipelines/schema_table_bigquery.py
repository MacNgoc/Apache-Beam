from apache_beam.io.gcp.internal.clients import bigquery

      
table_schema = bigquery.TableSchema()

# Fields that use standard types.
#FL_DATE:date
fl_date_schema = bigquery.TableFieldSchema()
fl_date_schema.name = 'FL_DATE'
fl_date_schema.type = 'date'
fl_date_schema.mode = 'nullable'
table_schema.fields.append(fl_date_schema)

#UNIQUE_CARRIER:string
unique_carrier_schema = bigquery.TableFieldSchema()
unique_carrier_schema.name = 'UNIQUE_CARRIER'
unique_carrier_schema.type = 'string'
unique_carrier_schema.mode = 'nullable'
table_schema.fields.append(unique_carrier_schema)


#AIRLINE_ID:string
airline_id_schema = bigquery.TableFieldSchema()
airline_id_schema.name = 'AIRLINE_ID'
airline_id_schema.type = 'string'
airline_id_schema.mode = 'nullable'
table_schema.fields.append(airline_id_schema)

#CARRIER:string
carrier_schema = bigquery.TableFieldSchema()
carrier_schema.name = 'CARRIER'
carrier_schema.type = 'string'
carrier_schema.mode = 'nullable'
table_schema.fields.append(carrier_schema)

#FL_NUM:string
fl_number_schema = bigquery.TableFieldSchema()
fl_number_schema.name = 'FL_NUM'
fl_number_schema.type = 'string'
fl_number_schema.mode = 'nullable'
table_schema.fields.append(fl_number_schema)

#ORIGIN_AIRPORT_ID:string
origin_airport_id_schema = bigquery.TableFieldSchema()
origin_airport_id_schema.name = 'ORIGIN_AIRPORT_ID'
origin_airport_id_schema.type = 'string'
origin_airport_id_schema.mode = 'nullable'
table_schema.fields.append(origin_airport_id_schema)

#ORIGIN_AIRPORT_SEQ_ID:integer
origin_airport_seq_id_schema = bigquery.TableFieldSchema()
origin_airport_seq_id_schema.name = 'ORIGIN_AIRPORT_SEQ_ID'
origin_airport_seq_id_schema.type = 'integer'
origin_airport_seq_id_schema.mode = 'nullable'
table_schema.fields.append(origin_airport_seq_id_schema)

#ORIGIN_CITY_MARKET_ID:string
origin_city_market_id_schema = bigquery.TableFieldSchema()
origin_city_market_id_schema.name = 'ORIGIN_CITY_MARKET_ID'
origin_city_market_id_schema.type = 'string'
origin_city_market_id_schema.mode = 'nullable'
table_schema.fields.append(origin_city_market_id_schema)

#ORIGIN:string
origin_schema = bigquery.TableFieldSchema()
origin_schema.name = 'ORIGIN'
origin_schema.type = 'string'
origin_schema.mode = 'nullable'
table_schema.fields.append(origin_schema)

#DEST_AIRPORT_ID:string
dest_airport_id_schema = bigquery.TableFieldSchema()
dest_airport_id_schema.name = 'DEST_AIRPORT_ID'
dest_airport_id_schema.type = 'string'
dest_airport_id_schema.mode = 'nullable'
table_schema.fields.append(dest_airport_id_schema)

#DEST_AIRPORT_SEQ_ID:integer
dest_airport_seq_id_schema = bigquery.TableFieldSchema()
dest_airport_seq_id_schema.name = 'DEST_AIRPORT_SEQ_ID'
dest_airport_seq_id_schema.type = 'integer'
dest_airport_seq_id_schema.mode = 'nullable'
table_schema.fields.append(dest_airport_seq_id_schema)


#DEST_CITY_MARKET_ID:string
dest_city_market_id_schema = bigquery.TableFieldSchema()
dest_city_market_id_schema.name = 'DEST_CITY_MARKET_ID'
dest_city_market_id_schema.type = 'string'
dest_city_market_id_schema.mode = 'nullable'
table_schema.fields.append(dest_city_market_id_schema)

#DEST:string
dest_schema = bigquery.TableFieldSchema()
dest_schema.name = 'DEST'
dest_schema.type = 'string'
dest_schema.mode = 'nullable'
table_schema.fields.append(dest_schema)


#CRS_DEP_TIME:timestamp, CRS_DEP_TIME:timestamp
crs_dep_time_schema = bigquery.TableFieldSchema()
crs_dep_time_schema.name = 'CRS_DEP_TIME'
crs_dep_time_schema.type = 'timestamp'
crs_dep_time_schema.mode = 'nullable'
table_schema.fields.append(crs_dep_time_schema)


#DEP_TIME:timestamp, 
dep_time_schema = bigquery.TableFieldSchema()
dep_time_schema.name = 'DEP_TIME'
dep_time_schema.type = 'timestamp'
dep_time_schema.mode = 'nullable'
table_schema.fields.append(dep_time_schema)



#DEP_DELAY:float, DEP_DELAY:float
dep_delay_schema = bigquery.TableFieldSchema()
dep_delay_schema.name = 'DEP_DELAY'
dep_delay_schema.type = 'float'
dep_delay_schema.mode = 'nullable'
table_schema.fields.append(dep_delay_schema)


#TAXI_OUT:float, TAXI_OUT:float
taxi_out_schema = bigquery.TableFieldSchema()
taxi_out_schema.name = 'TAXI_OUT'
taxi_out_schema.type = 'float'
taxi_out_schema.mode = 'nullable'
table_schema.fields.append(taxi_out_schema)


#WHEELS_OFF:timestamp, WHEELS_OFF:timestamp
wheels_off_schema = bigquery.TableFieldSchema()
wheels_off_schema.name = 'WHEELS_OFF'
wheels_off_schema.type = 'timestamp'
wheels_off_schema.mode = 'nullable'
table_schema.fields.append(wheels_off_schema)


#WHEELS_ON:timestamp,
wheels_on_schema = bigquery.TableFieldSchema()
wheels_on_schema.name = 'WHEELS_ON'
wheels_on_schema.type = 'timestamp'
wheels_on_schema.mode = 'nullable'
table_schema.fields.append(wheels_on_schema)


#TAXI_IN:float,
taxi_in_schema = bigquery.TableFieldSchema()
taxi_in_schema.name = 'TAXI_IN'
taxi_in_schema.type = 'float'
taxi_in_schema.mode = 'nullable'
table_schema.fields.append(taxi_in_schema)


#CRS_ARR_TIME:timestamp,
crs_arr_time_schema = bigquery.TableFieldSchema()
crs_arr_time_schema.name = 'CRS_ARR_TIME'
crs_arr_time_schema.type = 'timestamp'
crs_arr_time_schema.mode = 'nullable'
table_schema.fields.append(crs_arr_time_schema)


#ARR_TIME:timestamp,
arr_time_schema = bigquery.TableFieldSchema()
arr_time_schema.name = 'ARR_TIME'
arr_time_schema.type = 'timestamp'
arr_time_schema.mode = 'nullable'
table_schema.fields.append(arr_time_schema)


#ARR_DELAY:float,
arr_delay_schema = bigquery.TableFieldSchema()
arr_delay_schema.name = 'ARR_DELAY'
arr_delay_schema.type = 'float'
arr_delay_schema.mode = 'nullable'
table_schema.fields.append(arr_delay_schema)



#CANCELLED:string,
cancelled_schema = bigquery.TableFieldSchema()
cancelled_schema.name = 'CANCELLED'
cancelled_schema.type = 'string'
cancelled_schema.mode = 'nullable'
table_schema.fields.append(cancelled_schema)



#CANCELLATION_CODE:string,
cancellation_code_schema = bigquery.TableFieldSchema()
cancellation_code_schema.name = 'CANCELLATION_CODE'
cancellation_code_schema.type = 'string'
cancellation_code_schema.mode = 'nullable'
table_schema.fields.append(cancellation_code_schema)


#DIVERTED:string,
diverted_schema = bigquery.TableFieldSchema()
diverted_schema.name = 'DIVERTED'
diverted_schema.type = 'string'
diverted_schema.mode = 'nullable'
table_schema.fields.append(diverted_schema)


#DISTANCE:float,
distance_schema = bigquery.TableFieldSchema()
distance_schema.name = 'DISTANCE'
distance_schema.type = 'float'
distance_schema.mode = 'nullable'
table_schema.fields.append(distance_schema)


#DEP_AIRPORT_LAT:float,
dep_airport_lat_schema = bigquery.TableFieldSchema()
dep_airport_lat_schema.name = 'DEP_AIRPORT_LAT'
dep_airport_lat_schema.type = 'float'
dep_airport_lat_schema.mode = 'nullable'
table_schema.fields.append(dep_airport_lat_schema)


#DEP_AIRPORT_LON:float,
dep_airport_lon_schema = bigquery.TableFieldSchema()
dep_airport_lon_schema.name = 'DEP_AIRPORT_LON'
dep_airport_lon_schema.type = 'float'
dep_airport_lon_schema.mode = 'nullable'
table_schema.fields.append(dep_airport_lon_schema)


#DEP_AIRPORT_TZOFFSET:float,
dep_airport_tzoffset_schema = bigquery.TableFieldSchema()
dep_airport_tzoffset_schema.name = 'DEP_AIRPORT_TZOFFSET'
dep_airport_tzoffset_schema.type = 'float'
dep_airport_tzoffset_schema.mode = 'nullable'
table_schema.fields.append(dep_airport_tzoffset_schema)


#ARR_AIRPORT_LAT:float,
arr_airport_lat_schema = bigquery.TableFieldSchema()
arr_airport_lat_schema.name = 'ARR_AIRPORT_LAT'
arr_airport_lat_schema.type = 'float'
arr_airport_lat_schema.mode = 'nullable'
table_schema.fields.append(arr_airport_lat_schema)


#ARR_AIRPORT_LON:float,
arr_airport_lon_schema = bigquery.TableFieldSchema()
arr_airport_lon_schema.name = 'ARR_AIRPORT_LON'
arr_airport_lon_schema.type = 'float'
arr_airport_lon_schema.mode = 'nullable'
table_schema.fields.append(arr_airport_lon_schema)


#ARR_AIRPORT_TZOFFSET:float,
arr_airport_tzoffset_schema = bigquery.TableFieldSchema()
arr_airport_tzoffset_schema.name = 'ARR_AIRPORT_TZOFFSET'
arr_airport_tzoffset_schema.type = 'float'
arr_airport_tzoffset_schema.mode = 'nullable'
table_schema.fields.append(arr_airport_tzoffset_schema)


#EVENT:string,
event_schema = bigquery.TableFieldSchema()
event_schema.name = 'EVENT_TYPE'
event_schema.type = 'string'
event_schema.mode = 'nullable'
table_schema.fields.append(event_schema)

#NOTIFY_TIME:timestamp
notify_time_schema = bigquery.TableFieldSchema()
notify_time_schema.name = 'EVENT_TIME'
notify_time_schema.type = 'timestamp'
notify_time_schema.mode = 'nullable'
table_schema.fields.append(notify_time_schema)


#EVENT_DATA: strings
event_data_schema = bigquery.TableFieldSchema()
event_data_schema.name = 'EVENT_DATA'
event_data_schema.type = 'string'
event_data_schema.mode = 'nullable'
table_schema.fields.append(event_data_schema)
