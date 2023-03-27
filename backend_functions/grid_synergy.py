# from firebase_admin import firestore
class GridSynergy():
    '''
    Class for generating grid synergies
    Initial development: Jan 2021
    '''
    def __init__(self, json_dir='grid_synergy_dict.json', weights_dir='industry_weights.json'):
        '''
        Read in grid JSON and store in dictionary
        '''
        import json
        import os
        #print(os.getcwd())
        with open(json_dir, 'r') as fp:
            self.grid_dict = json.load(fp)

        with open(weights_dir, 'r') as fp:
            self.weights = json.load(fp)
        
    def naive_grid(self, lhs_industries, rhs_industries):
        '''
        INPUTS
        lhs_industries: a list of industries on left hand side of matching
        rhs_industries: a list of industries on right hand side of matching
        MODIFIES
        N/A
        RETURNS
        final_synergies: a list of unique synergies generated from all possible lhs 
                   and rhs industry pairs based on the grid database
        '''

        #* Eshita implementation of grid synergies
        # will replace eventually with dictionary based json
        final_synergies = set()
        for l in lhs_industries:
            for grid_elem in self.grid_dict:
                if l == grid_elem['']:
                    for r in rhs_industries:
                        try:
                            edited_row = grid_elem[r].split(',')
                            for e in edited_row:
                                if e == '':
                                    continue
                                final_synergies.add(e.strip())
                        except:
                            print("ERROR IN INDUSTRY", r)
        
        for r in rhs_industries:
            for grid_elem in self.grid_dict:
                if r == grid_elem['']:
                    for l in lhs_industries:
                        try:
                            edited_row = grid_elem[l].split(',')
                            for e in edited_row:
                                if e == '':
                                    continue
                                final_synergies.add(e.strip())
                        except:
                            print("ERROR IN INDUSTRY", l)

        #* Implement weights
        w_ = -.5
        for l in lhs_industries:
            for r in rhs_industries:
                if l in self.weights:
                    if r in self.weights[l]:
                        if self.weights[l][r] > w_:
                            w_ = self.weights[l][r]

        return (list(final_synergies), w_)

#Testing calls
if __name__ == "__main__":
    import time
    g = GridSynergy()

    start = time.time()
    for i in range(1000):
        f = g.naive_grid(['rideshare'], ['telemedicine / medicine'])
        # f = g.naive_grid(['telemedicine / medicine'], ['rideshare'])
    print(f)
    print("total time", time.time() - start)