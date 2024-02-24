import json

def process_stock_data(input_file, output_file):
    best_bids = []
    best_asks = []
    results = []
    ARRAY_MAX_LENGTH = 4
    
    # Function to update or insert an order array
    def update_or_insert(array, new_order, compare_fn):
        found = False
        for i in range(len(array)):
            if array[i]['price'] == new_order['price']:
                array[i]['size'] += new_order['size']
                found = True
                break
        
        if not found:
            insert_sorted(array, new_order, compare_fn)

        if len(array) > ARRAY_MAX_LENGTH:
            array.pop()
            
    # Function to insert element in a sorted order
    def insert_sorted(array, element, compare_fn):
        insertion_index = len(array)
        for i in range(len(array)):
            if compare_fn(element, array[i]) < 0:
                insertion_index = i
                break
        array.insert(insertion_index, element)

    # Function to handle cancellation
    def handle_cancellation(array, canceled_order):
        for i in range(len(array)):
            if array[i]['order_id'] == canceled_order['order_id']:
                array[i]['size'] -= canceled_order['size']
                if array[i]['size'] <= 0:
                    array.pop(i)
                break
            
   # Reading the file
    with open(input_file, 'r') as file:
        for line in file:
            order = json.loads(line)
            print(order)  # For debugging, you can remove this later
            action = order.get('action')
            if action == 'A':
                price = int(order.get('price', 0)) / 1e9  # Using 0 as a default value for price
                size = int(order.get('size', 0))  # Using 0 as a default value for size
                order_id = order.get('order_id')
                side = order.get('side')

                if side == 'A':
                    update_or_insert(best_asks, {'price': price, 'size': size, 'order_id': order_id},
                                    lambda a, b: a['price'] - b['price'])
                elif side == 'B':
                    update_or_insert(best_bids, {'price': price, 'size': size, 'order_id': order_id},
                                    lambda a, b: b['price'] - a['price'])

                spread = best_asks[0]['price'] - best_bids[0]['price'] if best_asks and best_bids else None
                result = {
                    'action': action,
                    'side': side,
                    'price': price,
                    'size': size,
                    'order_id': order_id,
                    'best_bids': best_bids[:],  # Copy the list to avoid reference issues
                    'best_asks': best_asks[:],  # Copy the list to avoid reference issues
                    'spread': spread,
                }
                ts_event = order.get('ts_event')
                if ts_event is not None:
                    result['ts_event'] = ts_event

                results.append(result)
        
    # Writing to the output file in NDJSON format
    with open(output_file, 'w') as file:
        for result in results:
            json.dump(result, file)
            file.write('\n')  # Write a newline character after each JSON object
    print("Finished processing and writing to the file.")




input_file ="../stock-data/smci/XNAS-20240217-DR3J9CCF3H/xnas-itch-20231218.mbo.json"
output_file = './outputs/output3.json'

process_stock_data(input_file, output_file)
