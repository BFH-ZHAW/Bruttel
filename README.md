# Bruttel
Spark Code für die Masterarbeit
Zweiter Branche zum Testen dier GitHub Funktionalität

Testen:- Git Verzeichnis Downloaden
	   - Im Projektverzeichnis(Bruttel) kompilieren:  mvn package
	   - Auführen mit: spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 --class com.bruttel.DataFrameCSVKomplett /home/cloudera/git/Bruttel/target/FirstSpark-0.0.1.jar local file:///home/cloudera/git/Bruttel/files/PortfolioKomplett.csv 
	   