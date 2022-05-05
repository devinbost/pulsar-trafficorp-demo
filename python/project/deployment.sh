# Update the requirements.txt file:
pipreqs /Users/dbost/proj/datastax_astra_demo --force

pip3 download -r requirements.txt -d /Users/dbost/proj/datastax_astra_demo/deps
zip -r code ./ -x .env