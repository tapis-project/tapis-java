#/bin/bash
# Drop database

echo "DROP DATABASE junkdb" | psql --host=localhost --username=postgres
echo "DROP DATABASE junkdb2" | psql --host=localhost --username=postgres
echo "DROP DATABASE junkdb3" | psql --host=localhost --username=postgres
