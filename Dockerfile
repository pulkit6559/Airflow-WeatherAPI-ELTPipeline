# Dockerfile

# Use the official PostgreSQL image
FROM postgres:latest

# Make the database persistent
VOLUME ["./postgres_data"]

# Expose the PostgreSQL port
EXPOSE ${POSTGRES_PORT}

# Start PostgreSQL
CMD ["postgres"]
