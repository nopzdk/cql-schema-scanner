file = open("cql_schema.txt", "r")
# Program to read all the lines in a file using readline() function
# creating counts for what we are looking for
cql_type = 0
cql_table = 0
cql_index = 0
cql_mv = 0
cql_keyspace = 0
bloom_filter_fp_chance = 0
caching = 0
compaction = 0
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
    if 'CREATE TABLE' in content:
        cql_table += 1
        print(f'{content}')
        #print(content)
    if 'CREATE INDEX' in content:
        cql_index += 1
        print(f'{content}')
        #print(content)
    if 'CREATE MATERIALIZED VIEW' in content:
        cql_mv += 1
        print(f'{content}')
        #print(content)
    if 'CREATE TYPE' in content:
        cql_type+=1
        #print(content)
    if 'CREATE KEYSPACE' in content:
        cql_keyspace += 1
        #print(content)
    if 'bloom_filter_fp_chance' in content:
        if '0.01' not in content:
            bloom_filter_fp_chance += 1
            print(content)
    if 'caching' in content:
        if 'NONE' in content:
            caching += 1
            print(content)
    if 'compaction' in content:
        if 'IncrementalCompactionStrategy' not in content:
            compaction += 1
            print(content)
        if 'space_amplification_goal' in content:
            space_amp += 1
            print(content)
    if 'compression' in content:
        if 'org.apache.cassandra.io.compress.LZ4Compressor' not in content:
            compression += 1
            print(content)
    if 'crc_check_chance' in content:
        if '1.0' not in content:
            crc_check_chance += 1
            print(content)
    if 'dclocal_read_repair_chance' in content:
        if '0.0' not in content:
            dclocal_read_repair_chance += 1
            print(content)
    if 'default_time_to_live' in content:
        #TODO fix detection
        if '0' not in content:
            default_time_to_live += 1
            print(content)
    if 'gc_grace_seconds' in content:
        if '864000' not in content:
            gc_grace_seconds += 1
            print(content)
    if 'max_index_interval' in content:
        if '2048' not in content:
            max_index_interval += 1
            print(content)
    if 'memtable_flush_period_in_ms' in content:
        #TODO fix logic
        if '0' not in content:
            memtable_flush_period_in_ms += 1
            print(content)
    if 'min_index_interval' in content:
        if '128' not in content:
            min_index_interval += 1
            print(content)
    if 'read_repair_chance' in content:
        #TODO fix logic
        if '0.0' not in content:
            read_repair_chance += 1
            print(content)
    if 'speculative_retry' in content:
        if '99.0PERCENTILE' not in content:
            speculative_retry += 1
            print(content)
#printing results
print(f'TYPES: {cql_type}')
print(f'KEYSPACES: {cql_keyspace}')

print(f'TABLES: {cql_table}')
if bloom_filter_fp_chance > 0:
    print(f'{bloom_filter_fp_chance} table(s) with NON DEFAULT bloom_filter_fp_chance')
if caching > 0:
    print(f'{caching} table(s) with  NON DEFAULT caching')
if compaction > 0:
    print(f'{compaction} table(s) with NON ICS compaction')
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
if min_index_interval > 0:
    print(f'{min_index_interval} table(s) with NON Default min_index_interval')
if read_repair_chance > 0:
    print(f'{read_repair_chance} table(s) with NON Default read_repair_chance')
if speculative_retry > 0:
    print(f'{speculative_retry} table(s) with NON Default speculative_retry')
file.close()