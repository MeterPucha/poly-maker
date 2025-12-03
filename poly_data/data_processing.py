import json
from sortedcontainers import SortedDict
import poly_data.global_state as global_state
import poly_data.CONSTANTS as CONSTANTS

from trading import perform_trade
import time 
import asyncio
from poly_data.data_utils import set_position, set_order, update_positions

def process_book_data(asset, json_data):
    global_state.all_data[asset] = {
        'asset_id': json_data['asset_id'],  # token_id for the Yes token
        'bids': SortedDict(),
        'asks': SortedDict()
    }

    global_state.all_data[asset]['bids'].update({float(entry['price']): float(entry['size']) for entry in json_data['bids']})
    global_state.all_data[asset]['asks'].update({float(entry['price']): float(entry['size']) for entry in json_data['asks']})

def process_price_change(asset, asset_id, side, price_level, new_size):
    if asset_id != global_state.all_data[asset]['asset_id']:
        return  # skip updates for the No token to prevent duplicated updates
    if side == 'bids':
        book = global_state.all_data[asset]['bids']
    else:
        book = global_state.all_data[asset]['asks']

    if new_size == 0:
        if price_level in book:
            del book[price_level]
    else:
        book[price_level] = new_size

def process_data(json_datas, trade=True):
    import json

    # Normalize input: can be string, dict, or list
    if isinstance(json_datas, str):
        # Try to parse JSON string
        try:
            json_datas = json.loads(json_datas)
        except json.JSONDecodeError:
            #print("[market_ws] Skipping non-JSON message:", repr(json_datas))
            return

    # If it's a single dict, wrap it in a list
    if isinstance(json_datas, dict):
        json_datas = [json_datas]
    # If it's something weird (not list/dict/str), bail
    elif not isinstance(json_datas, list):
        print("[market_ws] Unexpected payload type:", type(json_datas), repr(json_datas))
        return

    for json_data in json_datas:
        #print("[market_ws] RAW:", repr(json_data))

        # We only care about dict messages
        if not isinstance(json_data, dict):
            print("[market_ws] Skipping non-dict element:", repr(json_data))
            continue

        event_type = json_data.get('event_type')
        if event_type is None:
            print("[market_ws] Missing event_type in message:", repr(json_data))
            continue

        asset = json_data.get('market')
        if asset is None:
            print("[market_ws] Missing market in message:", repr(json_data))
            continue
        
        if event_type == 'book':
            process_book_data(asset, json_data)

            if trade:
                asyncio.create_task(perform_trade(asset))
                
        elif event_type == 'price_change':
            for data in json_data['price_changes']:
                asset_id = data.get('asset_id')

                # Defensive: if there is no asset_id, skip this entry
                if asset_id is None:
                    print("[market_ws] price_change without asset_id:", repr(data))
                    continue


                
                side = 'bids' if data['side'] == 'BUY' else 'asks'
                price_level = float(data['price'])
                new_size = float(data['size'])
                process_price_change(asset, asset_id, side, price_level, new_size)

                if trade:
                    asyncio.create_task(perform_trade(asset))
        

        print(f'Received book update for {asset}:', global_state.all_data[asset])

def add_to_performing(col, id):
    if col not in global_state.performing:
        global_state.performing[col] = set()
    
    if col not in global_state.performing_timestamps:
        global_state.performing_timestamps[col] = {}

    # Add the trade ID and track its timestamp
    global_state.performing[col].add(id)
    global_state.performing_timestamps[col][id] = time.time()

def remove_from_performing(col, id):
    if col in global_state.performing:
        global_state.performing[col].discard(id)

    if col in global_state.performing_timestamps:
        global_state.performing_timestamps[col].pop(id, None)

def process_user_data(rows):
    if not isinstance(rows, list):
        print(f"User data received but it's not a list: {rows}")  # Log for debugging
        rows = [rows] if isinstance(rows, dict) else []
        #return  # Skip processing
        
    for row in rows:
        market = row['market']

        side = row['side'].lower()
        token = row['asset_id']
            
        if token in global_state.REVERSE_TOKENS:     
            col = token + "_" + side

            if row['event_type'] == 'trade':
                size = 0
                price = 0
                maker_outcome = ""
                taker_outcome = row['outcome']

                is_user_maker = False
                for maker_order in row['maker_orders']:
                    if maker_order['maker_address'].lower() == global_state.client.browser_wallet.lower():
                        print("User is maker")
                        size = float(maker_order['matched_amount'])
                        price = float(maker_order['price'])
                        
                        is_user_maker = True
                        maker_outcome = maker_order['outcome'] #this is curious

                        if maker_outcome == taker_outcome:
                            side = 'buy' if side == 'sell' else 'sell' #need to reverse as we reverse token too
                        else:
                            token = global_state.REVERSE_TOKENS[token]
                
                if not is_user_maker:
                    size = float(row['size'])
                    price = float(row['price'])
                    print("User is taker")

                print("TRADE EVENT FOR: ", row['market'], "ID: ", row['id'], "STATUS: ", row['status'], " SIDE: ", row['side'], "  MAKER OUTCOME: ", maker_outcome, " TAKER OUTCOME: ", taker_outcome, " PROCESSED SIDE: ", side, " SIZE: ", size) 


                if row['status'] == 'CONFIRMED' or row['status'] == 'FAILED' :
                    if row['status'] == 'FAILED':
                        print(f"Trade failed for {token}, decreasing")
                        asyncio.create_task(asyncio.sleep(2))
                        update_positions()
                    else:
                        remove_from_performing(col, row['id'])
                        print("Confirmed. Performing is ", len(global_state.performing[col]))
                        print("Last trade update is ", global_state.last_trade_update)
                        print("Performing is ", global_state.performing)
                        print("Performing timestamps is ", global_state.performing_timestamps)
                        
                        asyncio.create_task(perform_trade(market))

                elif row['status'] == 'MATCHED':
                    add_to_performing(col, row['id'])

                    print("Matched. Performing is ", len(global_state.performing[col]))
                    set_position(token, side, size, price)
                    print("Position after matching is ", global_state.positions[str(token)])
                    print("Last trade update is ", global_state.last_trade_update)
                    print("Performing is ", global_state.performing)
                    print("Performing timestamps is ", global_state.performing_timestamps)
                    asyncio.create_task(perform_trade(market))
                elif row['status'] == 'MINED':
                    remove_from_performing(col, row['id'])

            elif row['event_type'] == 'order':
                print("ORDER EVENT FOR: ", row['market'], " STATUS: ",  row['status'], " TYPE: ", row['type'], " SIDE: ", side, "  ORIGINAL SIZE: ", row['original_size'], " SIZE MATCHED: ", row['size_matched'])
                
                set_order(token, side, float(row['original_size']) - float(row['size_matched']), row['price'])
                asyncio.create_task(perform_trade(market))

    else:
        print(f"User date received for {market} but its not in")
