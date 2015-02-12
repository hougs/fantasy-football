Start Hive using:

	hive -i udfs.hql

to load the Exhibit JAR and UDFs. The supernova.hql
code builds the tables in the super_football database,
and the superquery.hql is an example of using LATERAL
VIEWs and Exhibit within UDTFs to compute many statistics
in a single pass over the data.
