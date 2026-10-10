import os
import re

def migrate():
    print("Starting MongoDB v2 migration...")

    # 1. Update go.mod
    go_mod_path = 'go.mod'
    if os.path.exists(go_mod_path):
        with open(go_mod_path, 'r') as f:
            content = f.read()
        # Replace any v1.x.x with v2.5.0
        new_content = re.sub(r'go\.mongodb\.org/mongo-driver v1\.\d+\.\d+', 'go.mongodb.org/mongo-driver/v2 v2.5.0', content)
        if new_content != content:
            with open(go_mod_path, 'w') as f:
                f.write(new_content)
            print(f"Updated {go_mod_path}")

    # 2. Walk and update .go files
    updated_count = 0
    for root, dirs, files in os.walk('.'):
        if '.git' in dirs:
            dirs.remove('.git')
        if 'vendor' in dirs:
            dirs.remove('vendor')

        for file in files:
            if file.endswith('.go'):
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()

                orig = content

                # A. Update Import Paths
                content = content.replace('"go.mongodb.org/mongo-driver/mongo/options"', '"go.mongodb.org/mongo-driver/v2/mongo/options"')
                content = content.replace('"go.mongodb.org/mongo-driver/mongo/readpref"', '"go.mongodb.org/mongo-driver/v2/mongo/readpref"')
                content = content.replace('"go.mongodb.org/mongo-driver/mongo"', '"go.mongodb.org/mongo-driver/v2/mongo"')
                content = content.replace('"go.mongodb.org/mongo-driver/bson"', '"go.mongodb.org/mongo-driver/v2/bson"')

                # B. Update mongo.Connect signature (removes context parameter)
                # Matches: mongo.Connect(ctx, opts) or mongo.Connect(context.Background(), opts)
                content = re.sub(r'mongo\.Connect\s*\(\s*ctx\s*,\s*', 'mongo.Connect(', content)
                content = re.sub(r'mongo\.Connect\s*\(\s*context\.[A-Za-z]+\(\)\s*,\s*', 'mongo.Connect(', content)

                # C. Update Disconnect -> Close (Per RFP Section 2.5)
                # NOTE: The RFP specifies renaming to Close(ctx).
                # If compilation fails here, revert to Disconnect(ctx) as v2 *Client docs still show Disconnect.
                content = content.replace('.Disconnect(ctx)', '.Close(ctx)')
                content = content.replace('.Disconnect(context.', '.Close(context.')

                # D. Update classifyBulkErr signature in sink/docdb/docdb.go
                if 'sink/docdb/docdb.go' in path.replace('\\', '/'):
                    content = content.replace('func classifyBulkErr(we mongo.WriteError', 'func classifyBulkErr(we mongo.BulkWriteError')

                if content != orig:
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Updated {path}")
                    updated_count += 1

    print(f"\nMigration complete. Updated {updated_count} files.")
    print("Next steps:")
    print("1. Run: go mod tidy")
    print("2. Run: go build ./...")
    print("3. Check classify_test.go for manual struct updates (see documentation).")

if __name__ == '__main__':
    migrate()