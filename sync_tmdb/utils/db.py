def insert_into(cursor, table: str, columns: list, temp_table: str, on_conflict: list, on_conflict_update: list):
	if len(on_conflict_update) > 0:
		update_clause = f"DO UPDATE SET {','.join([f'{column}=EXCLUDED.{column}' for column in on_conflict_update])}"
	else:
		update_clause = "DO NOTHING"
	cursor.execute(f"""
		INSERT INTO {table} ({','.join(columns)})
		SELECT {','.join(columns)} FROM {temp_table}
		ON CONFLICT ({','.join(on_conflict)}) {update_clause};
	""")