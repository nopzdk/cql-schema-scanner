file = open("cql_schema.txt", "r")
# Program to read all the lines in a file using readline() function
# creating counts for what we are looking for

counts = {
    "types": 0,
    "keyspaces": 0,
    "tables": 0,
    "indexes": 0,
    "mws": 0,
    "bloom_filter_fp_chance": 0,
    "caching": 0,
    "compaction": 0,

}


space_amp = 0
compression = 0
crc_check_chance = 0
dclocal_read_repair_chance = 0
default_time_to_live = 0
gc_grace_seconds = 0
max_index_interval = 0
memtable_flush_period_in_ms = 0
min_index_interval = 0
read_repair_chance = 0
speculative_retry = 0


while True:
    content=file.readline()
    if not content:
        break

    if 'CREATE KEYSPACE' in content:
        counts["keyspaces"] += 1
        print(content.rstrip())

    if 'CREATE TABLE' in content:
        counts["tables"] += 1
        print(content.rstrip())

    if 'CREATE INDEX' in content:
        counts["indexes"] += 1
#        print(content.rstrip())

    if 'CREATE MATERIALIZED VIEW' in content:
        counts["mws"] += 1
#        print(content.rstrip())

    if 'CREATE TYPE' in content:
        counts["types"] += 1

    if 'bloom_filter_fp_chance' in content:
        if '0.01' not in content:
            counts["bloom_filter_fp_chance"] += 1
            print(content.rstrip())

    if 'caching' in content:
        if 'NONE' in content:
            counts["caching"] += 1
            print(content.rstrip())

    if 'compaction' in content:
        if 'IncrementalCompactionStrategy' not in content:
            counts["compaction"] += 1
            print(content.rstrip())
        if 'space_amplification_goal' in content:
            space_amp += 1
            print(content.rstrip())

    if 'compression' in content:
        if 'org.apache.cassandra.io.compress.LZ4Compressor' not in content:
            compression += 1
            print(content.rstrip())

    if 'crc_check_chance' in content:
        if '1.0' not in content:
            crc_check_chance += 1
            print(content.rstrip())

    if 'dclocal_read_repair_chance' in content:
        if not content.rstrip().endswith(" 0.0"):
            dclocal_read_repair_chance += 1
            print(content.rstrip())

    if 'default_time_to_live' in content:
        if not content.rstrip().endswith(" 0"):
            default_time_to_live += 1
            print(content.rstrip())

    if 'gc_grace_seconds' in content:
        if '864000' not in content:
            gc_grace_seconds += 1
            print(content.rstrip())

    if 'max_index_interval' in content:
        if '2048' not in content:
            max_index_interval += 1
            print(content.rstrip())

    if 'memtable_flush_period_in_ms' in content:
        if not content.rstrip().endswith(" 0"):
            memtable_flush_period_in_ms += 1
            print(content.rstrip())

    if 'min_index_interval' in content:
        if '128' not in content:
            min_index_interval += 1
            print(content.rstrip())

    if ' read_repair_chance' in content:
        if not content.rstrip().endswith(" 0.0"):
            read_repair_chance += 1
            print(content.rstrip())

    if 'speculative_retry' in content:
        if '99.0PERCENTILE' not in content:
            speculative_retry += 1
            print(content.rstrip())

#printing results
print(f'TYPES: {counts["types"]}')
print(f'KEYSPACES: {counts["keyspaces"]}')
print(f'TABLES: {counts["tables"]}')
print(f'INDEXES: {counts["indexes"]}')
print(f'MWs: {counts["mws"]}')

if counts["bloom_filter_fp_chance"] > 0:
    print(f'{counts["bloom_filter_fp_chance"]} table(s) with NON Default bloom_filter_fp_chance')
if counts["caching"] > 0:
    print(f'{counts["caching"]} table(s) with NON Default caching')
if counts["compaction"] > 0:
    print(f'{counts["compaction"]} table(s) with NON ICS compaction')
if space_amp > 0:
    print(f'{space_amp} table(s) with space_amplification_goal')
if compression > 0:
    print(f'{compression} table(s) with NON Default sstable_compression')
if crc_check_chance > 0:
    print(f'{crc_check_chance} table(s) with NON Default crc_check_chance')
if dclocal_read_repair_chance > 0:
    print(f'{dclocal_read_repair_chance} table(s) with NON Default dclocal_read_repair_chance')
if default_time_to_live > 0:
    print(f'{default_time_to_live} table(s) with NON Default default_time_to_live')
if gc_grace_seconds > 0:
    print(f'{gc_grace_seconds} table(s) with NON Default gc_grace_seconds')
if max_index_interval > 0:
    print(f'{max_index_interval} table(s) with NON Default max_index_interval')
if memtable_flush_period_in_ms > 0:
    print(f'{memtable_flush_period_in_ms} table(s) with NON Default memtable_flush_period_in_ms')
if min_index_interval > 0:
    print(f'{min_index_interval} table(s) with NON Default min_index_interval')
if read_repair_chance > 0:
    print(f'{read_repair_chance} table(s) with NON Default read_repair_chance')
if speculative_retry > 0:
    print(f'{speculative_retry} table(s) with NON Default speculative_retry')
file.close()