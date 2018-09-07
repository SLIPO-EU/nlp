def run(jar, input, output_tomtom_extractor, yelp_output_csv, output_directory_yelp, output_directory_nlp):
    print("Ready")
    run_tomtomextractor(jar, input, output_tomtom_extractor)
    run_yelp_query(output_tomtom_extractor, output_directory_yelp, output_csv)
    run_keyword_extraction(yelp_output_csv)
    print("keyword extraction done")
    run_rdf_gen('resources/yelp_business_nouns_withallcategories.csv', output_directory_nlp) #business_nouns is output file of keyword extraction


def run_rdf_gen(input, output_directory_nlp):
    import rdf_gen
    rdf_gen.run_keywords(input)
    rdf_gen.run_sentiments(output_directory_yelp, output_directory_nlp) #sentiment and NLP analysis is done here, and in the same step written to the rdf output file
    rdf_gen.to_unicode() #this function is used to show all unicode characters correctly, as the original .nt file does not show them correct


def run_tomtomextractor(jar, input, output_tomtom_extractor): # call subprocess to run the tomtom extractor jar
    import subprocess
    print("call tomtomextractor.jar")
    subprocess.call('java -jar ' + jar + " " + input + " " + output_tomtom_extractor, shell=True)
    print("finished tomtomextractor.jar")


def run_yelp_query(output_tomtom_extractor, output_directory_yelp, output_csv): #it is only possible to make ~2500 queries per day
    import yelp_core
    import config as cfg
    print("call yelp query")
    try:
        print('Test')
        yelp_core.run(output_tomtom_extractor,
                  output_directory_yelp,
                  output_csv,
                  token='Bearer ' + cfg.token['token1'],
                  )
    except:
        print("ERROR - too many requests1")
    try:
        print('Test2')
        yelp_core.run(output_tomtom_extractor,
                  output_directory_yelp,
                  output_csv,
                  token='Bearer ' + cfg.token['token2'],
                  )
    except:
        print("ERROR - too many requests2")
    try:
        print('Test3')
        yelp_core.run(output_tomtom_extractor,
                  output_directory_yelp,
                  output_csv,
                  token='Bearer ' + cfg.token['token3'],
                  )
    except:
        print("ERROR - too many requests3")
    try:
        print('Test4')
        yelp_core.run(output_tomtom_extractor,
                  output_directory_yelp,
                  output_csv,
                  token='Bearer ' + cfg.token['token4'],
                  )
    except:
        print("ERROR - too many requests4")
    print("finished yelp query")


def run_keyword_extraction(yelp_output_csv):
    print("call keyword extraction")
    import yelp_reviews_preprocessing
    clear_csv()
    yelp_reviews_preprocessing.main(yelp_output_csv)
    import candidate_generation
    candidate_generation.run()
    print("finished keyword extraction")


def clear_csv():  # clear all temporary csv files before running another round of keyword extraction
    filename = "resources/business_nouns.csv"
    f = open(filename, "w+")
    f.close()
    filename = "resources/preprocessed_results.csv"
    f = open(filename, "w+")
    f.close()
    filename = "resources/review_score.csv"
    f = open(filename, "w+")
    f.write("poiid,score")
    f.close()


jar = "resources/tomtomextractor.jar"  # location of tomtomjar
input = "resources/tomtom_pois_austria_v0.3.nt"   # original tomtom file with pois ontology
output_tomtom_extractor = "resources/tomtom_pois_austria_v0.3_unique.csv"  # file where tomtom extractor stores data.csv, input for yelp queries
output_directory_yelp = "resources/output_all"  # directory where yelp entries are saved as files,input for nlp analysis
output_csv = "resources/yelp_result_all.csv"  # csv file - output of yelp queries, input for keyword extraction
output__directory_nlp = "resources/output_new"  # output directory for nlp- scala/spark output


run(jar, input, output_tomtom_extractor, output_csv, output_directory_yelp, output__directory_nlp)

