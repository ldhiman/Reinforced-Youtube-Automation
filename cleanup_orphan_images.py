import os
from db import get_connection
import time

STORAGE = "storage/memes"


def cleanup_orphan_images():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT image_path
        FROM memes
        WHERE image_path IS NOT NULL
    """)

    # Normalize DB paths
    db_files = {
        os.path.normpath(row[0])
        for row in cursor.fetchall()
    }

    conn.close()

    scanned = 0
    deleted = 0

    for root, _, files in os.walk(STORAGE):
        for f in files:

            path = os.path.normpath(os.path.join(root, f))
            scanned += 1

            if path not in db_files and (time.time() - os.path.getmtime(path)) > 86400:
                try:
                    os.remove(path)
                    deleted += 1
                    print(f"Deleted orphan: {path}")

                except Exception as e:
                    print(f"Failed deleting {path}: {e}")

    print(f"\nCleanup complete")
    print(f"Files scanned: {scanned}")
    print(f"Files deleted: {deleted}")


if __name__ == "__main__":
    cleanup_orphan_images()