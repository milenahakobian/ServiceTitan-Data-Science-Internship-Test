import pickle
import pandas as pd

class DataExtractor(object):
    def __init__(self, pickled_path, expired_invoices_path):
        self.pickled_path = pickled_path
        self.expired_invoices_path = expired_invoices_path
        
    
    def load_data(self):
        '''
        a function for loading the data
        '''
        with open(self.pickled_path, 'rb') as file:
            self.invoices = pickle.load(file)
         
        with open(self.expired_invoices_path, 'r') as file:
            expired_invoices_str = file.read().strip()
            self.expired_invoices = {int(id.strip()) for id in expired_invoices_str.split(',')}


    def convert_type(self, type_id):
        type_dict = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        return type_dict.get(type_id, 'Unknown')
    
    def word_to_number(self, word):
        numbers = {
            'one': 1,
            'two': 2,
            'three': 3,
            'four': 4,
            'five': 5,
            'six': 6,
            'seven': 7,
            'eight': 8,
            'nine': 9,
            'ten': 10
            # Add more mappings as needed
        }
        if isinstance(word, str):
            return numbers.get(word.lower(), 0)
        else:
            return word

    
    def transform_data(self):
        '''
        a function for transforming the data into a flat data
        '''

        # create a list to hold the flattened data
        data = []
        
        for invoice in self.invoices:
            invoice_id = invoice['id'] 
            created_on = pd.to_datetime(invoice['created_on'])
            invoice_total = sum(int(item['item']['unit_price']) * self.word_to_number(item['quantity']) for item in invoice['items'])

            # check for expired
            is_expired = invoice_id in self.expired_invoices

            # iterating over the items in an invoice
            for item in invoice['items']:
                invoiceitem_id = item['item']['id']
                invoiceitem_name = item['item']['name']
                item_type = self.convert_type(item['item']['type'])
                unit_price = int(item['item']['unit_price'])
                quantity = self.word_to_number(item['quantity'])
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / invoice_total if invoice_total else 0

                # appending to the list
                data.append({
                        'invoice_id': invoice_id,
                        'created_on': created_on,
                        'invoiceitem_id': invoiceitem_id,
                        'invoiceitem_name': invoiceitem_name,
                        'type': item_type,
                        'unit_price': unit_price,
                        'total_price': total_price,
                        'percentage_in_invoice': percentage_in_invoice,
                        'is_expired': is_expired
                    })
                
        df = pd.DataFrame(data)

        # sorting by 
        df.sort_values(by=['invoice_id', 'invoiceitem_id'], inplace=True)

        return df


    def save_to_csv(self, csv_path):
        df = self.transform_data()
        df.to_csv(csv_path, index=False)

data_extractor = DataExtractor('invoices_new.pkl', 'expired_invoices.txt')
data_extractor.load_data()
data_extractor.save_to_csv('output.csv')
