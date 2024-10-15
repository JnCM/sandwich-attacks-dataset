import multiprocessing, time, json, pymongo, pytz, os, requests, gzip
from datetime import datetime
import pandas as pd
import logging

def init_process():
    global mongo_connection
    uri = "mongodb://localhost:27017"
    mongo_connection = pymongo.MongoClient(uri, maxPoolSize=None)

def collect_data(attacks):
    collection = mongo_connection["sandwich-attacks"]["attacks"]
    
    for attack in attacks:
        attack_formatted = json.loads(attack)
        
        result = collection.find_one({"first_transaction.hash": attack_formatted["first_transaction"]["hash"]})
        if result is None:
            try:
                block_datetime = datetime.fromtimestamp(attack_formatted["block_timestamp"], tz=pytz.UTC)
                mempool_filename = "Mempool/{}{:02d}{:02d}_{:02d}.csv.gz".format(block_datetime.year, block_datetime.month, block_datetime.day, block_datetime.hour)

                if not os.path.exists(mempool_filename):
                    url_blocknative = "https://archive.blocknative.com/{}{:02d}{:02d}/{:02d}.csv.gz".format(block_datetime.year, block_datetime.month, block_datetime.day, block_datetime.hour)
                    response = requests.get(url_blocknative, stream=True)
                    if response.status_code == 200:
                        f = open(mempool_filename, "wb")
                        f.write(response.raw.read())
                        f.close()
                    else:
                        logging.error("Error during blocknative API: {}".format(response.status_code))
                
                f = gzip.open(mempool_filename)
                df_mempool = pd.read_csv(f, sep="\t", low_memory=False)
                f.close()
                
                tx_hash_ta1 = attack_formatted["first_transaction"]["hash"]
                tx_hash_tv = attack_formatted["whale_transaction"]["hash"]
                tx_hash_ta2 = attack_formatted["second_transaction"]["hash"]

                if df_mempool.loc[(df_mempool["hash"] == tx_hash_ta1) & (df_mempool["status"] == "pending")].empty:
                    attack_formatted["first_transaction"]["visibility"] = "private"
                else:
                    attack_formatted["first_transaction"]["visibility"] = "public"
                
                if df_mempool.loc[(df_mempool["hash"] == tx_hash_tv) & (df_mempool["status"] == "pending")].empty:
                    attack_formatted["whale_transaction"]["visibility"] = "private"
                else:
                    attack_formatted["whale_transaction"]["visibility"] = "public"
                
                if df_mempool.loc[(df_mempool["hash"] == tx_hash_ta2) & (df_mempool["status"] == "pending")].empty:
                    attack_formatted["second_transaction"]["visibility"] = "private"
                else:
                    attack_formatted["second_transaction"]["visibility"] = "public"
                
                attack_formatted.pop("_id")

                result = collection.insert_one(attack_formatted)
                logging.info(f"Attack #{result.inserted_id} inserted!")
            except Exception as e:
                logging.error(f"Attack with Ta1 hash `{attack_formatted["first_transaction"]["hash"]}` cannot be inserted!")
                logging.error(f"Error: {e}")

def main():
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    with open("Dados/attacks_012023.json", "r", encoding="utf-8") as f:
        attacks = f.readlines()
    f.close()

    attack_count = len(attacks)
    logging.info("Attack count:", attack_count)

    n_cpus = multiprocessing.cpu_count()
    n = int(attack_count/n_cpus)

    slices_of_attacks = []
    count = 0
    for i in range(n_cpus):
        if i == 0:
            slices_of_attacks.append(attacks[:n])
        elif i == n_cpus-1:
            slices_of_attacks.append(attacks[count:])
        else:
            slices_of_attacks.append(attacks[count:count+n])
        count += n

    logging.info(f"Running collection of sandwich attacks with {n_cpus} CPUs")
    logging.info("Initializing workers...")
    execution_times = []
    multiprocessing.set_start_method('spawn')
    with multiprocessing.Pool(processes=n_cpus, initializer=init_process) as pool:
        start_total = time.time()
        execution_times += pool.map(collect_data, slices_of_attacks)
        end_total = time.time()
        logging.info(f"Total execution time: {end_total - start_total}")
        logging.info(execution_times)

if __name__ == "__main__":
    main()