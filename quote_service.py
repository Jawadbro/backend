import mysql.connector
from datetime import datetime, timedelta
import json
import uuid

def create_quote(customer_ref, lines):
    """
    Creates a new quote with the given customer reference and line items.
    Validates SKUs, quantities, and pricing config.
    Returns the new quote_id on success.
    Raises Exception with a user-friendly message on failure.
    """
    cnx = mysql.connector.connect(
        host='localhost',
        user='root',
        password='mahin1tanim2@',
        database='casa_rom_sales'
    )
    cursor = cnx.cursor(dictionary=True)

    try:
        cnx.start_transaction()

        # Validate customer_ref
        if not customer_ref or not isinstance(customer_ref, str):
            raise Exception("Customer reference must be a non-empty string.")

        # Validate lines
        if not lines or not isinstance(lines, list):
            raise Exception("Lines must be a non-empty list.")

        quote_id = 'CRQ-' + uuid.uuid4().hex[:8].upper()
        valid_until = datetime.now() + timedelta(hours=24)

        # Get pricing config
        cursor.execute("SELECT transfer_discount, installments_markup FROM config_pricing WHERE id = 1")
        config = cursor.fetchone()
        if not config:
            raise Exception("Pricing config not found in the database.")

        transfer_discount = float(config['transfer_discount'])
        installments_markup = float(config['installments_markup'])

        # Insert the quote first with placeholder totals
        cursor.execute("""
            INSERT INTO quotes (quote_id, customer_ref, valid_until, list_total, transfer_total, installments_total, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            quote_id,
            customer_ref,
            valid_until.strftime('%Y-%m-%d %H:%M:%S'),
            0,  # placeholder
            0,  # placeholder
            0,  # placeholder
            json.dumps(["Stock will be confirmed before fulfillment."])
        ))

        list_total = 0.0

        # Process and insert quote lines
        for idx, line in enumerate(lines):
            sku = line.get('sku')
            qty = line.get('qty')
            attrs = line.get('attributes', {})

            if not sku or not isinstance(sku, str):
                raise Exception(f"Line {idx+1}: SKU must be a non-empty string.")
            if not isinstance(qty, int) or qty <= 0:
                raise Exception(f"Line {idx+1}: Quantity must be a positive integer.")

            cursor.execute("SELECT unit_price, name FROM products WHERE sku = %s", (sku,))
            product = cursor.fetchone()
            if not product:
                raise Exception(f"Line {idx+1}: Invalid SKU '{sku}' (not found in products).")

            unit_price = float(product['unit_price'])
            name = product['name']

            line_total = unit_price * qty
            list_total += line_total

            cursor.execute("""
                INSERT INTO quote_lines (quote_id, line_number, sku, name, qty, unit_price, line_total, attrs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                quote_id,
                idx + 1,
                sku,
                name,
                qty,
                unit_price,
                line_total,
                json.dumps(attrs)
            ))

        # Calculate totals
        transfer_total = list_total * (1 - transfer_discount)
        installments_total = list_total * (1 + installments_markup)

        # Update the quote with the correct totals
        cursor.execute("""
            UPDATE quotes
            SET list_total = %s, transfer_total = %s, installments_total = %s
            WHERE quote_id = %s
        """, (
            list_total,
            transfer_total,
            installments_total,
            quote_id
        ))

        cnx.commit()
        return quote_id

    except Exception as e:
        cnx.rollback()
        # Optionally, log the error here
        raise Exception(f"Quote creation failed: {str(e)}")

    finally:
        cursor.close()
        cnx.close()
