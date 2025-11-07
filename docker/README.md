# Docker Development Environment

This directory contains the Docker setup for the federated query engine development environment.

## Components

### PostgreSQL
- **Image**: postgres:15-alpine
- **Port**: 5432
- **Database**: analytics
- **User**: postgres
- **Password**: postgres
- **Schemas**: public, staging

### Sample Data

The PostgreSQL database is initialized with sample data for testing:

#### Tables:
- **public.customers**: Customer information with regions (US, EU, APAC)
- **public.orders**: Order records with statuses and amounts
- **public.products**: Product catalog with categories
- **public.order_items**: Line items for each order
- **staging.temp_sales**: Temporary sales data for testing multi-schema queries

#### Sample Queries:
```sql
-- Total orders by region
SELECT c.region, COUNT(o.id) as total_orders, SUM(o.amount) as total_revenue
FROM public.customers c
JOIN public.orders o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.region;

-- Top products by revenue
SELECT p.name, SUM(oi.quantity * oi.unit_price) as revenue
FROM public.products p
JOIN public.order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name
ORDER BY revenue DESC
LIMIT 5;
```

## Usage

### Start the environment:
```bash
docker-compose up -d
```

### Stop the environment:
```bash
docker-compose down
```

### Remove all data (clean start):
```bash
docker-compose down -v
```

### View logs:
```bash
docker-compose logs -f postgres
```

### Connect to PostgreSQL:
```bash
docker-compose exec postgres psql -U postgres -d analytics
```

## Testing

After starting the environment, you can test the federated query engine with:

```bash
# Run tests
pytest tests/

# Run specific test file
pytest tests/test_datasources.py
```

## Configuration

Update the `config/example_config.yaml` to use the Docker PostgreSQL:

```yaml
datasources:
  postgres_prod:
    type: postgresql
    host: localhost  # or docker.host.internal on Mac/Windows
    port: 5432
    database: analytics
    user: postgres
    password: postgres
    schemas: [public, staging]
```
