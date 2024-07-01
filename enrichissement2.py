import requests




url = "https://api.insee.fr/entreprises/sirene/V3/siret/{siret}"

reponse = requests.get(url)
print(reponse)