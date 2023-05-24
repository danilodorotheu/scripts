from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)

def run_job(params):
    database_a_name = params['database_a_name']
    table_a_name = params['table_a_name']
    database_b_name = params['database_b_name']
    table_b_name = params['table_b_name']
    
    partitions_a_values = glueContext.get_partitions(database=database_a_name, table_name=table_a_name)
    partitions_a_values = [int(partition['Values'][0]) for partition in partitions_a_values]
    
    partitions_b_values = glueContext.get_partitions(database=database_b_name, table_name=table_b_name)
    partitions_b_values = [int(partition['Values'][0]) for partition in partitions_b_values]
    
    filtered_partitions = [partition for partition in partitions_a_values if partition not in partitions_b_values]
    sorted_partitions = sorted(filtered_partitions)
    
    for partition in sorted_partitions:
        predicate = f"partition_column={partition}"
        dynamic_frame_a = glueContext.create_dynamic_frame.from_catalog(database=database_a_name,
                                                                         table_name=table_a_name,
                                                                         transformation_ctx='dynamic_frame',
                                                                         push_down_predicate=predicate)
        dynamic_frame_b = glueContext.create_dynamic_frame.from_catalog(database=database_b_name,
                                                                         table_name=table_b_name,
                                                                         transformation_ctx='dynamic_frame')
        
        df_a = dynamic_frame_a.toDF()
        df_b = dynamic_frame_b.toDF()
        
        joined_df = df_a.join(df_b, 'ID', 'inner')
        filtered_df = joined_df.filter(joined_df['flag'] != 'D')
        insert_df = df_a.filter(df_a['flag'] == 'I')
        
        final_df = filtered_df.select(filtered_df.columns).union(insert_df)
        
        # Write final_df to table B
        glueContext.write_dynamic_frame.from_catalog(dynamic_frame=glueContext.create_dynamic_frame.fromDF(final_df, glueContext, "df"),
                                                      database=database_b_name,
                                                      table_name=table_b_name,
                                                      transformation_ctx='write_final_df')
