#wait for the SQL Server to come up
echo "waiting for database"
sleep 120s

echo "running set up script"
#run the setup script to create the DB and the schema in the DB
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Your_password123 -d master -i /src/setup.sql