import json

with open("temp_attacks_01.json", "r", encoding="utf-8") as f:
    attacks = json.load(f)

print(f"Total de ataques até o momento: {len(attacks)}")

count_tv_private = 0
count_tas_private = 0
count_tas_public = 0
count_ta1_public = 0
count_ta2_public = 0

for attack in attacks:
    if attack["first_transaction"]["visibility"] == "private" and attack["second_transaction"]["visibility"] == "private":
        count_tas_private += 1
    
    flag_total_public = False
    if attack["first_transaction"]["visibility"] == "public" and attack["second_transaction"]["visibility"] == "public":
        count_tas_public += 1
        flag_total_public = True
    
    if attack["first_transaction"]["visibility"] == "public" and not flag_total_public:
        count_ta1_public += 1
    
    if attack["second_transaction"]["visibility"] == "public" and not flag_total_public:
        count_ta2_public += 1

    if attack["whale_transaction"]["visibility"] == "private":
        count_tv_private += 1

print(f"Vítimas não encontradas na mempool: {count_tv_private}")
print(f"Ataques não encontrados na mempool: {count_tas_private}")
print(f"Ataques encontrados completamente na mempool: {count_tas_public}")
print(f"Ataques em que somente TA1 foi encontrada na mempool: {count_ta1_public}")
print(f"Ataques em que somente TA2 foi encontrada na mempool: {count_ta2_public}")