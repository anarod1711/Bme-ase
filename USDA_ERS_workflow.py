import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

long_food_market_sql = "select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 1 as food_id from USDA_ERS_workflow_staging.Food_1_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 2 as food_id from USDA_ERS_workflow_staging.Food_2_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 3 as food_id from USDA_ERS_workflow_staging.Food_3_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 4 as food_id from USDA_ERS_workflow_staging.Food_4_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 5 as food_id from USDA_ERS_workflow_staging.Food_5_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 6 as food_id from USDA_ERS_workflow_staging.Food_6_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 7 as food_id from USDA_ERS_workflow_staging.Food_7_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 8 as food_id from USDA_ERS_workflow_staging.Food_8_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 9 as food_id from USDA_ERS_workflow_staging.Food_9_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 10 as food_id from USDA_ERS_workflow_staging.Food_10_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 11 as food_id from USDA_ERS_workflow_staging.Food_11_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 12 as food_id from USDA_ERS_workflow_staging.Food_12_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 13 as food_id from USDA_ERS_workflow_staging.Food_13_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 14 as food_id from USDA_ERS_workflow_staging.Food_14_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 15 as food_id from USDA_ERS_workflow_staging.Food_15_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 16 as food_id from USDA_ERS_workflow_staging.Food_16_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 17 as food_id from USDA_ERS_workflow_staging.Food_17_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 18 as food_id from USDA_ERS_workflow_staging.Food_18_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 19 as food_id from USDA_ERS_workflow_staging.Food_19_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 20 as food_id from USDA_ERS_workflow_staging.Food_20_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 21 as food_id from USDA_ERS_workflow_staging.Food_21_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 22 as food_id from USDA_ERS_workflow_staging.Food_22_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 23 as food_id from USDA_ERS_workflow_staging.Food_23_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 24 as food_id from USDA_ERS_workflow_staging.Food_24_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 25 as food_id from USDA_ERS_workflow_staging.Food_25_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 26 as food_id from USDA_ERS_workflow_staging.Food_26_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 27 as food_id from USDA_ERS_workflow_staging.Food_27_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 28 as food_id from USDA_ERS_workflow_staging.Food_28_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 29 as food_id from USDA_ERS_workflow_staging.Food_29_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 30 as food_id from USDA_ERS_workflow_staging.Food_30_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 31 as food_id from USDA_ERS_workflow_staging.Food_31_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 32 as food_id from USDA_ERS_workflow_staging.Food_32_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 33 as food_id from USDA_ERS_workflow_staging.Food_33_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 34 as food_id from USDA_ERS_workflow_staging.Food_34_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 35 as food_id from USDA_ERS_workflow_staging.Food_35_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 36 as food_id from USDA_ERS_workflow_staging.Food_36_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 37 as food_id from USDA_ERS_workflow_staging.Food_37_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 38 as food_id from USDA_ERS_workflow_staging.Food_38_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 39 as food_id from USDA_ERS_workflow_staging.Food_39_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 40 as food_id from USDA_ERS_workflow_staging.Food_40_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 41 as food_id from USDA_ERS_workflow_staging.Food_41_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 42 as food_id from USDA_ERS_workflow_staging.Food_42_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 43 as food_id from USDA_ERS_workflow_staging.Food_43_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 44 as food_id from USDA_ERS_workflow_staging.Food_44_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 45 as food_id from USDA_ERS_workflow_staging.Food_45_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 46 as food_id from USDA_ERS_workflow_staging.Food_46_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 47 as food_id from USDA_ERS_workflow_staging.Food_47_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 48 as food_id from USDA_ERS_workflow_staging.Food_48_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 49 as food_id from USDA_ERS_workflow_staging.Food_49_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 50 as food_id from USDA_ERS_workflow_staging.Food_50_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 51 as food_id from USDA_ERS_workflow_staging.Food_51_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 52 as food_id from USDA_ERS_workflow_staging.Food_52_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 53 as food_id from USDA_ERS_workflow_staging.Food_53_Market UNION DISTINCT select GENERATE_UUID() as food_market_id, marketgroup as market_id, year, quarter, price, se as standard_error, n as sample_size, aggweight as agg_weight, totexp as tot_q_exp, 54 as food_id from USDA_ERS_workflow_staging.Food_54_Market;"
default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 3)
}

staging_dataset = 'USDA_ERS_workflow_staging'
modeled_dataset = 'USDA_ERS_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_food_categories_sql = 'create or replace table ' + modeled_dataset + '''.Food_Categories as select distinct *
                    from ''' + staging_dataset + '''.Food_Categories'''

create_food_market_sql = 'create or replace table ' + modeled_dataset + '''.Food_Market as ''' + long_food_market_sql

create_market_groups_sql = 'create or replace table ' + modeled_dataset + '''.Market_Groups as
                    select distinct market_id, market_name
                    from ''' + staging_dataset + '''.Market_Groups'''

create_foods_sql = 'create or replace table ' + modeled_dataset + '''.Foods as select distinct * from ''' + staging_dataset + '.Foods'''

with models.DAG(
        'USDA_ERS_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_success')
    
    #Only Beam transform that does not require instacart data is that of Food_Market_Beam_DF
    create_food_market2 = BashOperator(
            task_id='create_food_market2',
            bash_command='python /home/jupyter/airflow/dags/Food_Market_beam_dataflow2.py')
    
    create_food_market = BashOperator(
            task_id='create_food_market',
            bash_command=bq_query_start + "'" + create_food_market_sql + "'", 
            trigger_rule='all_done')
    
    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
        
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_food_categories = BashOperator(
            task_id='load_food_categories',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Food_Categories \
                         "gs://bmease/USDA_ERS/food_categories.csv"',
            trigger_rule='one_success')
    
    #Lines 63-439 are for loading each Food Market Table in staging (total 54 food market tables)
    load_food_market_1 = BashOperator(
                task_id='load_food_market_1',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_1_Market\
                             "gs://bmease/USDA_ERS/1.csv"',
                trigger_rule='one_success')

    load_food_market_2 = BashOperator(
                task_id='load_food_market_2',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_2_Market\
                             "gs://bmease/USDA_ERS/2.csv"',
                trigger_rule='one_success')

    load_food_market_3 = BashOperator(
                task_id='load_food_market_3',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_3_Market\
                             "gs://bmease/USDA_ERS/3.csv"',
                trigger_rule='one_success')

    load_food_market_4 = BashOperator(
                task_id='load_food_market_4',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_4_Market\
                             "gs://bmease/USDA_ERS/4.csv"',
                trigger_rule='one_success')

    load_food_market_5 = BashOperator(
                task_id='load_food_market_5',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_5_Market\
                             "gs://bmease/USDA_ERS/5.csv"',
                trigger_rule='one_success')

    load_food_market_6 = BashOperator(
                task_id='load_food_market_6',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_6_Market\
                             "gs://bmease/USDA_ERS/6.csv"',
                trigger_rule='one_success')

    load_food_market_7 = BashOperator(
                task_id='load_food_market_7',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_7_Market\
                             "gs://bmease/USDA_ERS/7.csv"',
                trigger_rule='one_success')

    load_food_market_8 = BashOperator(
                task_id='load_food_market_8',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_8_Market\
                             "gs://bmease/USDA_ERS/8.csv"',
                trigger_rule='one_success')

    load_food_market_9 = BashOperator(
                task_id='load_food_market_9',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_9_Market\
                             "gs://bmease/USDA_ERS/9.csv"',
                trigger_rule='one_success')

    load_food_market_10 = BashOperator(
                task_id='load_food_market_10',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_10_Market\
                             "gs://bmease/USDA_ERS/10.csv"',
                trigger_rule='one_success')

    load_food_market_11 = BashOperator(
                task_id='load_food_market_11',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_11_Market\
                             "gs://bmease/USDA_ERS/11.csv"',
                trigger_rule='one_success')

    load_food_market_12 = BashOperator(
                task_id='load_food_market_12',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_12_Market\
                             "gs://bmease/USDA_ERS/12.csv"',
                trigger_rule='one_success')

    load_food_market_13 = BashOperator(
                task_id='load_food_market_13',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_13_Market\
                             "gs://bmease/USDA_ERS/13.csv"',
                trigger_rule='one_success')

    load_food_market_14 = BashOperator(
                task_id='load_food_market_14',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_14_Market\
                             "gs://bmease/USDA_ERS/14.csv"',
                trigger_rule='one_success')

    load_food_market_15 = BashOperator(
                task_id='load_food_market_15',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_15_Market\
                             "gs://bmease/USDA_ERS/15.csv"',
                trigger_rule='one_success')

    load_food_market_16 = BashOperator(
                task_id='load_food_market_16',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_16_Market\
                             "gs://bmease/USDA_ERS/16.csv"',
                trigger_rule='one_success')

    load_food_market_17 = BashOperator(
                task_id='load_food_market_17',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_17_Market\
                             "gs://bmease/USDA_ERS/17.csv"',
                trigger_rule='one_success')

    load_food_market_18 = BashOperator(
                task_id='load_food_market_18',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_18_Market\
                             "gs://bmease/USDA_ERS/18.csv"',
                trigger_rule='one_success')

    load_food_market_19 = BashOperator(
                task_id='load_food_market_19',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_19_Market\
                             "gs://bmease/USDA_ERS/19.csv"',
                trigger_rule='one_success')

    load_food_market_20 = BashOperator(
                task_id='load_food_market_20',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_20_Market\
                             "gs://bmease/USDA_ERS/20.csv"',
                trigger_rule='one_success')

    load_food_market_21 = BashOperator(
                task_id='load_food_market_21',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_21_Market\
                             "gs://bmease/USDA_ERS/21.csv"',
                trigger_rule='one_success')

    load_food_market_22 = BashOperator(
                task_id='load_food_market_22',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_22_Market\
                             "gs://bmease/USDA_ERS/22.csv"',
                trigger_rule='one_success')

    load_food_market_23 = BashOperator(
                task_id='load_food_market_23',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_23_Market\
                             "gs://bmease/USDA_ERS/23.csv"',
                trigger_rule='one_success')

    load_food_market_24 = BashOperator(
                task_id='load_food_market_24',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_24_Market\
                             "gs://bmease/USDA_ERS/24.csv"',
                trigger_rule='one_success')

    load_food_market_25 = BashOperator(
                task_id='load_food_market_25',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_25_Market\
                             "gs://bmease/USDA_ERS/25.csv"',
                trigger_rule='one_success')

    load_food_market_26 = BashOperator(
                task_id='load_food_market_26',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_26_Market\
                             "gs://bmease/USDA_ERS/26.csv"',
                trigger_rule='one_success')

    load_food_market_27 = BashOperator(
                task_id='load_food_market_27',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_27_Market\
                             "gs://bmease/USDA_ERS/27.csv"',
                trigger_rule='one_success')

    load_food_market_28 = BashOperator(
                task_id='load_food_market_28',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_28_Market\
                             "gs://bmease/USDA_ERS/28.csv"',
                trigger_rule='one_success')

    load_food_market_29 = BashOperator(
                task_id='load_food_market_29',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_29_Market\
                             "gs://bmease/USDA_ERS/29.csv"',
                trigger_rule='one_success')

    load_food_market_30 = BashOperator(
                task_id='load_food_market_30',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_30_Market\
                             "gs://bmease/USDA_ERS/30.csv"',
                trigger_rule='one_success')

    load_food_market_31 = BashOperator(
                task_id='load_food_market_31',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_31_Market\
                             "gs://bmease/USDA_ERS/31.csv"',
                trigger_rule='one_success')

    load_food_market_32 = BashOperator(
                task_id='load_food_market_32',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_32_Market\
                             "gs://bmease/USDA_ERS/32.csv"',
                trigger_rule='one_success')

    load_food_market_33 = BashOperator(
                task_id='load_food_market_33',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_33_Market\
                             "gs://bmease/USDA_ERS/33.csv"',
                trigger_rule='one_success')

    load_food_market_34 = BashOperator(
                task_id='load_food_market_34',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_34_Market\
                             "gs://bmease/USDA_ERS/34.csv"',
                trigger_rule='one_success')

    load_food_market_35 = BashOperator(
                task_id='load_food_market_35',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_35_Market\
                             "gs://bmease/USDA_ERS/35.csv"',
                trigger_rule='one_success')

    load_food_market_36 = BashOperator(
                task_id='load_food_market_36',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_36_Market\
                             "gs://bmease/USDA_ERS/36.csv"',
                trigger_rule='one_success')

    load_food_market_37 = BashOperator(
                task_id='load_food_market_37',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_37_Market\
                             "gs://bmease/USDA_ERS/37.csv"',
                trigger_rule='one_success')

    load_food_market_38 = BashOperator(
                task_id='load_food_market_38',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_38_Market\
                             "gs://bmease/USDA_ERS/38.csv"',
                trigger_rule='one_success')

    load_food_market_39 = BashOperator(
                task_id='load_food_market_39',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_39_Market\
                             "gs://bmease/USDA_ERS/39.csv"',
                trigger_rule='one_success')

    load_food_market_40 = BashOperator(
                task_id='load_food_market_40',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_40_Market\
                             "gs://bmease/USDA_ERS/40.csv"',
                trigger_rule='one_success')

    load_food_market_41 = BashOperator(
                task_id='load_food_market_41',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_41_Market\
                             "gs://bmease/USDA_ERS/41.csv"',
                trigger_rule='one_success')

    load_food_market_42 = BashOperator(
                task_id='load_food_market_42',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_42_Market\
                             "gs://bmease/USDA_ERS/42.csv"',
                trigger_rule='one_success')

    load_food_market_43 = BashOperator(
                task_id='load_food_market_43',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_43_Market\
                             "gs://bmease/USDA_ERS/43.csv"',
                trigger_rule='one_success')

    load_food_market_44 = BashOperator(
                task_id='load_food_market_44',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_44_Market\
                             "gs://bmease/USDA_ERS/44.csv"',
                trigger_rule='one_success')

    load_food_market_45 = BashOperator(
                task_id='load_food_market_45',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_45_Market\
                             "gs://bmease/USDA_ERS/45.csv"',
                trigger_rule='one_success')

    load_food_market_46 = BashOperator(
                task_id='load_food_market_46',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_46_Market\
                             "gs://bmease/USDA_ERS/46.csv"',
                trigger_rule='one_success')

    load_food_market_47 = BashOperator(
                task_id='load_food_market_47',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_47_Market\
                             "gs://bmease/USDA_ERS/47.csv"',
                trigger_rule='one_success')

    load_food_market_48 = BashOperator(
                task_id='load_food_market_48',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_48_Market\
                             "gs://bmease/USDA_ERS/48.csv"',
                trigger_rule='one_success')

    load_food_market_49 = BashOperator(
                task_id='load_food_market_49',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_49_Market\
                             "gs://bmease/USDA_ERS/49.csv"',
                trigger_rule='one_success')

    load_food_market_50 = BashOperator(
                task_id='load_food_market_50',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_50_Market\
                             "gs://bmease/USDA_ERS/50.csv"',
                trigger_rule='one_success')

    load_food_market_51 = BashOperator(
                task_id='load_food_market_51',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_51_Market\
                             "gs://bmease/USDA_ERS/51.csv"',
                trigger_rule='one_success')

    load_food_market_52 = BashOperator(
                task_id='load_food_market_52',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_52_Market\
                             "gs://bmease/USDA_ERS/52.csv"',
                trigger_rule='one_success')

    load_food_market_53 = BashOperator(
                task_id='load_food_market_53',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_53_Market\
                             "gs://bmease/USDA_ERS/53.csv"',
                trigger_rule='one_success')

    load_food_market_54 = BashOperator(
                task_id='load_food_market_54',
                bash_command='bq --location=US load --autodetect --skip_leading_rows=1\
                             --source_format=CSV ' + staging_dataset + '.Food_54_Market\
                             "gs://bmease/USDA_ERS/54.csv"',
                trigger_rule='one_success')

    load_market_groups = BashOperator(
            task_id='load_market_groups',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Market_Groups \
                         "gs://bmease/USDA_ERS/market_group.csv"', 
            trigger_rule='one_success')
    
    load_FIPS_market_group = BashOperator(
            task_id='load_FIPS_market_group',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.FIPS_Market_Group \
                         "gs://bmease/USDA_ERS/FIPS_market_group.csv"', 
            trigger_rule='one_success')
    
    load_geo_market_group = BashOperator(
            task_id='load_geo_market_group',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Geo_Market_Group \
                         "gs://bmease/USDA_ERS/geo_market.csv"', 
            trigger_rule='one_success')
    
    load_geo_market = BashOperator(
            task_id='load_geo_market',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Geo_Market \
                         "gs://bmease/USDA_ERS/geo_master.csv"', 
            trigger_rule='one_success')
    
    load_geo_regions = BashOperator(
            task_id='load_geo_regions',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Geo_Regions \
                         "gs://bmease/USDA_ERS/geo_regions.csv"', 
            trigger_rule='one_success')
    
    load_geo_divisions = BashOperator(
            task_id='load_geo_divisions',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Geo_Divisions \
                         "gs://bmease/USDA_ERS/geo_divisions.csv"', 
            trigger_rule='one_success')
    
    load_state_codes = BashOperator(
            task_id='load_geo_divisions',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.State_Codes \
                         "gs://bmease/USDA_ERS/state_codes.csv"', 
            trigger_rule='one_success')
    

    load_foods = BashOperator(
            task_id='load_foods',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Foods \
                         "gs://bmease/USDA_ERS/foods.csv"', 
            trigger_rule='one_success')
    
    create_food_categories = BashOperator(
            task_id='create_food_categories',
            bash_command=bq_query_start + "'" + create_food_categories_sql + "'", 
            trigger_rule='one_success')
    
    create_market_groups = BashOperator(
            task_id='create_market_groups',
            bash_command=bq_query_start + "'" + create_market_groups_sql + "'", 
            trigger_rule='one_success')
    
    create_foods = BashOperator(
            task_id='create_foods',
            bash_command=bq_query_start + "'" + create_foods_sql + "'", 
            trigger_rule='one_success')
   
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
     
    create_staging >> create_modeled >> branch
    
    branch >>  load_FIPS_market_group
    
    branch >> load_geo_market_group
    
    branch >> load_geo_market
    
    branch >> load_geo_regions
    
    branch >> load_geo_divisions
    
    branch >> load_state_codes
    
    branch >> load_food_market_1 >> join    
    branch >> load_food_market_2 >> join
    branch >> load_food_market_3 >> join
    branch >> load_food_market_4 >> join
    branch >> load_food_market_5 >> join
    branch >> load_food_market_6 >> join
    branch >> load_food_market_7 >> join
    branch >> load_food_market_8 >> join
    branch >> load_food_market_9 >> join
    branch >> load_food_market_10 >> join
    branch >> load_food_market_11 >> join
    branch >> load_food_market_12 >> join
    branch >> load_food_market_13 >> join
    branch >> load_food_market_14 >> join
    branch >> load_food_market_15 >> join
    branch >> load_food_market_16 >> join
    branch >> load_food_market_17 >> join
    branch >> load_food_market_18 >> join
    branch >> load_food_market_19 >> join
    branch >> load_food_market_20 >> join
    branch >> load_food_market_21 >> join
    branch >> load_food_market_22 >> join
    branch >> load_food_market_23 >> join
    branch >> load_food_market_24 >> join
    branch >> load_food_market_25 >> join
    branch >> load_food_market_26 >> join
    branch >> load_food_market_27 >> join
    branch >> load_food_market_28 >> join
    branch >> load_food_market_29 >> join
    branch >> load_food_market_30 >> join
    branch >> load_food_market_31 >> join
    branch >> load_food_market_32 >> join
    branch >> load_food_market_33 >> join
    branch >> load_food_market_34 >> join
    branch >> load_food_market_35 >> join
    branch >> load_food_market_36 >> join
    branch >> load_food_market_37 >> join
    branch >> load_food_market_38 >> join
    branch >> load_food_market_39 >> join
    branch >> load_food_market_40 >> join
    branch >> load_food_market_41 >> join
    branch >> load_food_market_42 >> join
    branch >> load_food_market_43 >> join
    branch >> load_food_market_44 >> join
    branch >> load_food_market_45 >> join
    branch >> load_food_market_46 >> join
    branch >> load_food_market_47 >> join
    branch >> load_food_market_48 >> join
    branch >> load_food_market_49 >> join
    branch >> load_food_market_50 >> join
    branch >> load_food_market_51 >> join
    branch >> load_food_market_52 >> join
    branch >> load_food_market_53 >> join
    branch >> load_food_market_54 >> join
    
    branch >> load_food_categories >> create_food_categories  
    branch >> load_foods >> create_foods
    branch >> load_market_groups >> create_market_groups
    join >> create_food_market >> create_food_market2