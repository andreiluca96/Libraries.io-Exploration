import findspark
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

import schemas

findspark.init()
languages = ['Java', 'Python', 'R', 'C#', 'Objective-C', 'C', 'C++', 'Swift', 'Scala', 'Ruby', 'Julia', 'PHP',
             'JavaScript', 'bash', 'TypeScript', 'CoffeeScript']


# languages = ['Java', 'Python', 'R']


def init_spark():
    spark = SparkSession.builder \
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
    # pwrf = spark.read.csv(pwrf_path, header=True, timestampFormat=timestamp_format, schema=schemas.projects_with_repository_fields)

    projects = spark.read.csv(projects_path, header=True, timestampFormat=timestamp_format,
                              schema=schemas.projects)
    get_top_ten_projects_for_languages(projects, spark)
    exit(0)
    dependencies = spark.read.csv(dependencies_path, header=True, schema=schemas.dependencies, samplingRatio=0.1).drop(
        'ID')
    join_result = projects \
        .join(dependencies, projects['ID'] == dependencies['Project ID'])
    # .limit(5)
    print(join_result.take(5))
    count_result = join_result.groupBy('Repository ID').count()
    count_result.show()
    # print(count_result.where(projects['Repository ID']==5074).first())
    # print(projects.where(projects['ID']==5074).first())
    projects.join(count_result, projects['ID'] == count_result['Repository ID']).orderBy(
        'Latest Release Publish Timestamp', ascending=False).show()


def get_top_ten_projects_for_languages(projects, spark):
    languages_df = spark.createDataFrame(languages, projects.schema['Language'].dataType)
    projects_with_languages = projects.join(languages_df, projects['Language'] == languages_df['value'])
    # projects_with_languages.select('Language').distinct().show()
    window = Window.partitionBy('Language').orderBy(projects_with_languages['Dependent Repositories Count'].desc())
    agg_lang = projects_with_languages.withColumn('sorted_list', F.collect_list('Name').over(window))\
        .groupBy('Language').agg(F.max('sorted_list')).alias('sorted_list')
    # agg_lang.show()
    top_projects_by_language = [(row['Language'], row['max(sorted_list)'][:10]) for row in agg_lang.collect()]
    print(top_projects_by_language)
    with open('results/top_projects_by_language.txt', 'w+') as f:
        f.write(''.join([f'{str(p[0])}: {", ".join([l for l in p[1]])}\n' for p in top_projects_by_language]))


if __name__ == '__main__':
    main()
