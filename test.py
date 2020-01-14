import findspark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import schemas

languages = ['Java', 'Python', 'R', 'C#', 'Objective-C', 'C', 'C++', 'Swift', 'Scala', 'Ruby', 'Julia', 'PHP',
             'JavaScript', 'bash', 'TypeScript', 'CoffeeScript']


# languages = ['Java', 'Python', 'R']


def init_spark():
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('HelloWorld') \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.local.dir", "./tmp") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()
    # dummy_switch = '-1.4.0-2018-12-22'
    dummy_switch = '_dummy'

    projects_path = f'Libraries.io-open-data-1.4.0/libraries/projects-1.4.0-2018-12-22.csv'
    dependencies_path = f'Libraries.io-open-data-1.4.0/libraries/dependencies{dummy_switch}.csv'
    pwrf_path = f'Libraries.io-open-data-1.4.0/libraries/projects_with_repository_fields{dummy_switch}.csv'
    timestamp_format = 'YYYY-MM-DD HH:mm:ss z'
    # pwrf = spark.read.csv(pwrf_path, header=True, timestampFormat=timestamp_format, schema=schemas.projects_with_repository_fields)

    projects = spark.read.csv(projects_path, header=True, timestampFormat=timestamp_format,
                              schema=schemas.projects)
    # get_top_ten_projects_for_languages(projects, spark)
    dependencies = spark.read.csv(dependencies_path, header=True, schema=schemas.dependencies).drop('ID').select('Project ID', 'Dependency Project ID')
    dependencies = dependencies.limit(dependencies.count())
    dependency_pairs = dependencies.where(F.col('Dependency Project ID').isNotNull())
    pageranks = dependency_pairs \
        .select('Project ID') \
        .union(dependency_pairs
               .select('Dependency Project ID')) \
        .distinct().alias('ID') \
        .withColumn('pagerank', F.lit(1))
    dependency_counts = dependency_pairs.groupBy('Project ID').count()
    # print(pageranks.count(), pageranks.take(5))
    for it in range(2):
        print(f'It {it}')
        dependency_pairs = pageranks \
            .join(dependency_pairs, 'Project ID') \
            .join(dependency_counts, 'Project ID') \
            .withColumn('vote', F.col('pagerank') / F.col('count')) \
            .drop('pagerank', 'count')
        vote_sums = dependency_pairs \
            .groupBy('Dependency Project ID') \
            .sum('vote') \
            .withColumnRenamed('sum(vote)', 'vote_sum')
        # print(vote_sums.take(5))
        pageranks = pageranks \
            .join(vote_sums, pageranks['Project ID'] == vote_sums['Dependency Project ID'], how='left') \
            .drop('pagerank') \
            .drop('Dependency Project ID') \
            .withColumn('pagerank', 1 + F.col('vote_sum')) \
            .drop('vote_sum') \
            .fillna({'pagerank': 1})
        pageranks.show(10)
    pagerank_and_sourcerank = pageranks.join(projects.select('ID', 'SourceRank'), pageranks['Project ID'] == projects['ID'])\
        .drop('ID').drop('Project ID')
    print('Correlation:', pagerank_and_sourcerank.stat.corr('pagerank', 'SourceRank'))
    print('Covariance:', pagerank_and_sourcerank.stat.cov('pagerank', 'SourceRank'))
    exit(0)
    with(open('results/pageranks.txt', 'w+')) as f:
        f.write(pageranks)
        # pageranks = pageranks('pagerank', dependency_pairs.groupBy('Project ID').sum('vote'))

    # join_result = projects \
    #     .join(dependencies, projects['ID'] == dependencies['Project ID'])
    # .limit(5)
    # print(join_result.take(5))
    # count_result = join_result.groupBy('Repository ID').count()
    # count_result.show()
    # print(count_result.where(projects['Repository ID']==5074).first())
    # print(projects.where(projects['ID']==5074).first())
    # projects.join(count_result, projects['ID'] == count_result['Repository ID']).orderBy(
    #     'Latest Release Publish Timestamp', ascending=False).show()


def get_top_ten_projects_for_languages(projects, spark):
    languages_df = spark.createDataFrame(languages, projects.schema['Language'].dataType)

    top_projects_by_language = projects.select('Language', 'Name', 'Dependent Repositories Count') \
        .join(languages_df, languages_df['value'] == projects['Language']) \
        .orderBy(F.desc('Dependent Repositories Count')) \
        .groupBy('Language') \
        .agg(F.collect_list('Name')) \
        .rdd.map(list) \
        .map(lambda x: (x[0], x[1][:10])) \
        .collect()
    print(top_projects_by_language)
    with open('results/top_projects_by_language.txt', 'w+') as f:
        f.write(''.join([f'{str(p[0])}: {", ".join([l for l in p[1]])}\n' for p in top_projects_by_language]))


def most_used_tags(projects):
    split_col = projects\
        .withColumn('Keyword', F.explode(F.split(projects["Keywords"], ",")))\
        .drop('Keywords').groupBy('Keyword')\
        .count()\
        .orderBy(F.desc('count'))\
        .take(10)
    print(split_col)


if __name__ == '__main__':
    main()
