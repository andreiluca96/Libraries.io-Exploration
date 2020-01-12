import findspark
from pyspark.sql import SparkSession

import schemas

findspark.init()


def init_spark():
    spark = SparkSession.builder\
        .master('local[*]') \
        .appName('HelloWorld') \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.local.dir", "./tmp") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()
    dummy_switch = '-1.4.0-2018-12-22'
    # dummy_switch = '_dummy'

    projects_path = f'Libraries.io-open-data-1.4.0/libraries/projects{dummy_switch}.csv'
    dependencies_path = f'Libraries.io-open-data-1.4.0/libraries/dependencies_dummy.csv'
    pwrf_path = f'Libraries.io-open-data-1.4.0/libraries/projects_with_repository_fields{dummy_switch}.csv'
    timestamp_format = 'YYYY-MM-DD HH:mm:ss z'
    pwrf = spark.read.csv(pwrf_path, header=True, timestampFormat=timestamp_format, inferSchema=True).take(5)
    exit(0)
    projects = spark.read.csv(projects_path, header=True, timestampFormat=timestamp_format,
                              schema=schemas.projects)
    print('There are no brakes on the debug train')
    dependencies = spark.read.csv(dependencies_path, header=True, schema=schemas.dependencies, samplingRatio=0.1).drop('ID')
    print('Maybe some breaks on the debug train')
    join_result = projects \
        .join(dependencies, projects['ID'] == dependencies['Project ID'])
        # .limit(5)
    print(join_result.take(5))
    print('There are definitely brakes')
    count_result = join_result.groupBy('Repository ID').count()
    count_result.show()
    # print(count_result.where(projects['Repository ID']==5074).first())
    # print(projects.where(projects['ID']==5074).first())
    projects.join(count_result, projects['ID'] == count_result['Repository ID']).orderBy('Latest Release Publish Timestamp', ascending=False).show()


if __name__ == '__main__':
    main()
