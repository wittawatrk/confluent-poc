import re
import struct

DATA_TYPE_SIZE = {
    '00' : 1,
    '01' : 1,
    '02' : 2,
    '03' : 2,
    '04' : 4,
    '05' : 4,
    '06' : 4,
    '07' : 4,
    '08' : 2,
    '09' : 2,
    '10' : 2,
    '11' : 2,
}

SKIP_BYTES = 17

def swap(bytes, pattern):
    new_bytes = []
    for c in pattern:
        index  = ord(c) - ord("A")
        new_bytes.append(bytes[index])

    return new_bytes

def parse_bytes(hex, value_type):
    if value_type == 'INT32':
        return int(hex, 16)
    if value_type == 'FLOAT':
        return struct.unpack('!f',struct.pack('!I', int(hex, 16)))[0]

def get_value(byte_arr, pattern, value_type):
    swapped_bytes = swap(byte_arr, pattern)
    return parse_bytes("".join(swapped_bytes), value_type)

def get_hex_arr(hex_str):
    hex_data = hex_str.upper()
    n = 2
    hex_arr = [hex_data[i:i+n] for i in range(0, len(hex_data), n)]
    return hex_arr[SKIP_BYTES:]

def parse_1640001(hex_str):
    hex_arr = get_hex_arr(hex_str)

    data_type_index = 5
    channel_value_start_index = 9
    modbus_data = []
    while len(hex_arr) > 1 :
        channel_value_length = int(hex_arr[0], 16)
        data_type = hex_arr[data_type_index]
        end_index = channel_value_start_index + channel_value_length
        
        start_index = end_index - DATA_TYPE_SIZE[data_type]
        modbus_data.append(hex_arr[start_index:end_index])
        hex_arr = hex_arr[end_index:]
        
    # HARDCODE FOR POC
    configs = [
        ('voltage0', 1, "BADC", "FLOAT"),
        ("voltage1", 2, "BADC", "FLOAT"),
        ("voltage2", 3, "BADC", "FLOAT"),
        ("intensity0", 4, "BADC", "FLOAT"),
        ("intensity1", 5, "BADC", "FLOAT"),
        ("intensity2", 6, "BADC", "FLOAT"),
        ("kw", 7, "BADC", "FLOAT"),
        ("power_factor", 8, "BADC", "FLOAT"),
        ("watt_hour_counter", 9, "BADC", "INT32")
    ]
    data = {}
    for (key, channel, pattern, value_type) in configs:
        data[key] = get_value(modbus_data[channel-1], pattern, value_type)
    return data

if __name__ == '__main__':
    hex_data = '7ef4ea0002a1615e621300000000000000089600000005010100664360ac6743363c0896000000050101006743f5d56843b75f089600000005010100674306e46843e77c089600000005010100ae432a35a743dd7a089600000005010100b443d353ab4388a6089600000005010100ad433831a74374170896000000050101006048d5ea564810e3089600000005010100803f00007f3f3afe0896000000040100001600f9a9160002aa0096000000040100000096000000040100000096000000040100000096000000040100000096000000040100000096000000040100000096000000040100007e'
    print("input : {}".format(hex_data))
    data = parse_1640001(hex_data)
    print("output : {}".format(data))
