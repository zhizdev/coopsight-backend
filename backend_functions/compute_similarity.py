import spacy
import os
import random
import numpy as np
from firebase_admin import firestore
import uuid
from .grid_synergy import GridSynergy

class ComputeSimilarity:

    def __init__(self):
        # Load model
        # self.nlp = spacy.load('en_core_web_lg')
        self.nlp = spacy.load('en_core_web_md')
        os.environ['SPACY_WARNING_IGNORE'] = 'W008'
        self.GS = GridSynergy('backend_functions/grid_synergy_dict.json', 'backend_functions/industry_weights.json')

    def match(self, lhs, rhs):

        # Pairwise matching
        pairs = {}
        lhs2 = {}
        rhs2 = {}
        for l in lhs:
            lstring = ' '.join(lhs[l])
            if lstring == '':
                lstring = 'hm'
                lhs[l] = ['apple','pie','test','robot']

            lhs2[l] = self.nlp(lstring)

        for r in rhs:
            rstring = ' '.join(rhs[r])
            if rstring == '':
                rstring = 'hmm'
                rhs[r] = ['apple','pie','test','robot']

            rhs2[r] = self.nlp(rstring)

        counter = 0
        for l in lhs2:
            for r in rhs2:
                if l!=r:
                    sim = lhs2[l].similarity(rhs2[r])

                    print(l, r, sim)

                    # if sim == 0:
                    #     print('lhs')
                    #     for token in lhs2[l]:
                    #         print(token.text, token.has_vector, token.vector_norm, token.is_oov)
                    #     print('rhs')
                    #     for token in rhs2[r]:
                    #         print(token.text, token.has_vector, token.vector_norm, token.is_oov)
                    #     return pairs

                    # Compute key words
                    word_dict = {}
                    temp_dict = {}
                    limiter = 0
                    for w in lhs[l]:
                        if len(w.split()) <= 3:
                            word = self.nlp(w)
                            cos = rhs2[r].similarity(word)
                            temp_dict[w.lower()] = cos
                            limiter += 1
                            if limiter > 60:
                                break
                    limiter = 0
                    for w2 in rhs[r]:
                        if len(w2.split()) <= 3:
                            word2 = self.nlp(w2)
                            cos = lhs2[l].similarity(word2)
                            temp_dict[w2.lower()] = cos
                            limiter += 1
                            if limiter > 60:
                                break

                    # Exact word match
                    lhs_set = set()
                    rhs_set = set()
                    for temp in lhs[l]:
                        lhs_set.add(temp.lower())
                    for temp in rhs[r]:
                        rhs_set.add(temp.lower())
                    exact_list = lhs_set.intersection(rhs_set)
                    for item in exact_list:
                        word_dict[item] = 1


                    sorted_dict = sorted(temp_dict.items(), key=lambda x: x[1], reverse=True)
                    j = 0
                    for match_words in sorted_dict:
                        if j < 20:
                            word_dict[str(match_words[0])] = float(match_words[1])
                            j += 1


                    pairs[counter] = {'from':l, 'to':r, 'score':float(sim), 'matchwords':word_dict}
                    counter += 1

        # for p in pairs:
        #     print(pairs[p])

        return pairs

    def match_simple(self, lhs, rhs):
    
        # Pairwise matching
        pairs = {}
        lhs2 = {}
        rhs2 = {}
        for l in lhs:
            lstring = ' '.join(lhs[l])
            if lstring == '':
                lstring = 'hm'
                lhs[l] = ['apple','pie','test','robot']

            lhs2[l] = self.nlp(lstring)

        for r in rhs:
            rstring = ' '.join(rhs[r])
            if rstring == '':
                rstring = 'hmm'
                rhs[r] = ['apple','pie','test','robot']

            rhs2[r] = self.nlp(rstring)

        counter = 0
        for l in lhs2:
            for r in rhs2:
                if l!=r:
                    sim = lhs2[l].similarity(rhs2[r])

                    print(l, r, sim)

                    # Compute key words
                    word_dict = {}
                    temp_dict = {}

                    # Exact word match
                    lhs_set = set()
                    rhs_set = set()
                    for temp in lhs[l]:
                        lhs_set.add(temp.lower())
                    for temp in rhs[r]:
                        rhs_set.add(temp.lower())
                    exact_list = lhs_set.intersection(rhs_set)
                    for item in exact_list:
                        word_dict[item] = 1


                    pairs[counter] = {'from':l, 'to':r, 'score':float(sim), 'matchwords':word_dict}
                    counter += 1

        # for p in pairs:
        #     print(pairs[p])

        return pairs


    '''
    BACKGROUND MATCH UPDATE FUNCTIONS
    '''
    def fetch_sim_update(self, firebase_db, ecoid, docid):
        lhs = {}
        rhs = {}

        # list all docs within ecoid
        entities = firebase_db.collection(u'ecosystemMatching/' + ecoid + '/keywords')
        for entity in entities.stream():
            entity_dict = entity.to_dict()
            if entity_dict['docid'] != docid:
                temp_keywords = list(entity_dict['topk'].keys())
                rhs[entity_dict['docid']] = temp_keywords

            else:
                temp_keywords = list(entity_dict['topk'].keys())
                lhs[entity_dict['docid']] = temp_keywords 

        # print(lhs)
        # print(rhs)

        return lhs, rhs 

    def execute(self, database, firebase_db, vcdocid, connection, expressdocid=None):
        return "success", 200

    def update(self, database, firebase_db, path, newdocid, flag='default'):
        '''
        1) Delete all previous entries with newdocid
        2) Recompute and populate all matches with newdocid
        '''
        if path[0] == '/':
            path = path[1:]
        first_slash = path.find('/')
        ecoid = path[first_slash+1:path.find('/', first_slash+1)]

        # Batch delete old entries
        batch = firebase_db.batch()
        ecodoc = firebase_db.collection(u'ecosystemMatching/' + ecoid + '/matching')
        old_entries1 =  ecodoc.where('from', '==', newdocid).stream()
        old_entries2 =  ecodoc.where('to', '==', newdocid).stream()
        batch_counter = 0
        for doc in old_entries1:
            batch.delete(doc.reference)
            batch_counter += 1
            if batch_counter > 250:
                batch.commit()
                batch = firebase_db.batch()
                batch_counter = 0
        batch.commit()
        batch_counter = 0
        for doc in old_entries2:
            batch.delete(doc.reference)
            batch_counter += 1
            if batch_counter > 250:
                batch.commit()
                batch = firebase_db.batch()
                batch_counter = 0
        batch.commit()

        # Compute similarity
        lhs, rhs = self.fetch_sim_update(firebase_db, ecoid, newdocid)
        if flag == 'simple':
            pairs = self.match_simple(lhs, rhs)
        else:
            pairs = self.match(lhs, rhs)

        # Add new entries
        batch_counter = 0
        for p in pairs:
            new_doc1 = {
                'from':pairs[p]['from'],
                'to':pairs[p]['to'],
                'score':pairs[p]['score'],
                'matchwords':pairs[p]['matchwords'],
            }
            new_doc2 = {
                'from':pairs[p]['to'],
                'to':pairs[p]['from'],
                'score':pairs[p]['score'],
                'matchwords':pairs[p]['matchwords'],
            }
            new_ref1 = ecodoc.document()
            batch.set(new_ref1, new_doc1)
            new_ref2 = ecodoc.document()
            batch.set(new_ref2, new_doc2)
            batch_counter += 2
            if batch_counter > 248:
                batch.commit()
                batch = firebase_db.batch()
                batch_counter = 0
        batch.commit() 

        return "success", 200

    def fetch_open_db(self, firebase_db, from_value, match, from_type):
        '''
        Fetch the keywords for a set from open network
        '''
        lhs = {}
        rhs = {}
        lhs_20 = {}
        rhs_20 = {}
        match_type = ''

        #! Use docid to fetch match froms
        for from_id in from_value:
            entity_stream = firebase_db.collection(u'keywords').where(u'docid', u'==', from_id).stream()
            for entity in entity_stream:
                entity_dict = entity.to_dict()
                lhs[from_id] = list(entity_dict['topn'].keys())
                lhs_20[from_id] = list(entity_dict['topk'].keys())
            # print(from_id)

        #! Determine if match is a startup, corporation, or investor
        to_label = ''
        to_parent = ''
        entity_stream = firebase_db.collection(u'keywords').where(u'docid', u'==', match).stream()
        for entity in entity_stream:
            entity_dict = entity.to_dict()
            
            to_parent = match
            to_label = entity_dict['label']

            if to_label == 'startup' and from_type == 'single':
                #* one to one
                # print("one to one")
                match_type = 'one2one'
                rhs[match] = list(entity_dict['topn'].keys())
                rhs_20[match] = list(entity_dict['topk'].keys())
                to_parent = match
                break
            
            if from_type == 'single':
                #* one to many
                match_type = 'one2many'
                sub_stream = firebase_db.collection(u'keywords').where(u'parent', u'==', to_parent).stream()
                for sub in sub_stream:
                    sub_dict = sub.to_dict()
                    rhs[sub_dict['docid']] = list(sub_dict['topk'].keys())
                    rhs_20[sub_dict['docid']] = list(sub_dict['topk'].keys())
                # sub_stream = firebase_db.collection(u'keywords').where(u'docid', u'==', to_parent).stream()
                # for sub in sub_stream:
                #     sub_dict = sub.to_dict()
                #     rhs[sub_dict['docid']] = list(sub_dict['topk'].keys())
                #     rhs_20[sub_dict['docid']] = list(sub_dict['topk'].keys())
            else:
                # * many to one
                match_type = 'many2one'
                temp_list = []
                sub_stream = firebase_db.collection(u'keywords').where(u'parent', u'==', to_parent).stream()
                for sub in sub_stream:
                    sub_dict = sub.to_dict()
                    temp_list.extend(list(sub_dict['topk'].keys()))
                sub_stream = firebase_db.collection(u'keywords').where(u'docid', u'==', to_parent).stream()
                for sub in sub_stream:
                    sub_dict = sub.to_dict()
                    temp_list.extend(list(sub_dict['topk'].keys()))
                rhs[to_parent] = temp_list
                rhs_20[to_parent] = temp_list
            break

        #print(lhs)
        #print(rhs)
        lhs_cache = {}
        rhs_cache = {}
        lhs_doc = {}
        rhs_doc = {}
        lhs_20_doc = {}
        rhs_20_doc = {}
        for l in lhs:
            lhs_cache[l] = {}
            l_string = ' '.join(lhs[l])
            ls = self.nlp(l_string)
            lhs_doc[l] = np.array(ls.vector)

            l_20 = ' '.join(lhs_20[l])
            l_20 = self.nlp(l_20)
            lhs_20_doc[l] = np.array(l_20.vector)
            for word in lhs[l]:
                word = word.lower()
                w = self.nlp(word)
                if w.has_vector:
                    lhs_cache[l][word] = np.array(w.vector)
        for r in rhs:
            rhs_cache[r] = {}
            r_string = ' '.join(rhs[r])
            rs = self.nlp(r_string)
            rhs_doc[r] = np.array(rs.vector)

            l_20 = ' '.join(rhs_20[r])
            l_20 = self.nlp(l_20)
            rhs_20_doc[r] = np.array(l_20.vector)
            for word in rhs[r]:
                word = word.lower()
                w = self.nlp(word)
                if w.has_vector:
                    rhs_cache[r][word] = np.array(w.vector)
        
        
        #! Get matchwords
        sim_list = []
        pairs = {}
        for l in lhs_cache:
            pairs[l] = {}
            for r in rhs_cache:
                pairs[l][r] = {}
                temp_pairs = {}
                limiter = 0
                for w1 in lhs_cache[l]:
                    for w2 in rhs_cache[r]:
                        if len(w1.split()) <= 3 and len(w2.split()) <= 3:
                            a = lhs_cache[l][w1]
                            b = rhs_cache[r][w2]
                            sim = np.dot(a, b)/(np.linalg.norm(a)*np.linalg.norm(b))
                            if not np.isnan(sim):
                                temp_pairs[w1] = sim
                                temp_pairs[w2] = sim
                
                #! Sort dictionary
                word_dict = {}
                sorted_dict = sorted(temp_pairs.items(), key=lambda x: x[1], reverse=True)
                j = 0
                for match_words in sorted_dict:
                    if j < 20:
                        word_dict[str(match_words[0])] = float(match_words[1])
                        j += 1
                
                #! Store
                a = lhs_20_doc[l]
                b = rhs_20_doc[r]
                sim = np.dot(a, b)/(np.linalg.norm(a)*np.linalg.norm(b))
                sim_list.append(sim)
                pairs[l][r]['words'] = word_dict
                pairs[l][r]['sim'] = float(sim)
        
        #! Get total similarity score
        sim_list = np.array(sim_list)
        sim_avg = float(np.mean(sim_list))
        sim_max = float(np.mean(sim_list))

        #print(pairs)

        #! Generate synergies
        lhs_industry = {}
        rhs_industry = {}
        for l in lhs_cache:
            entity_stream = firebase_db.collection(u'industries').where(u'docid', u'==', l).stream()
            for entity in entity_stream:
                entity_dict = entity.to_dict()
                if 'industry' in entity_dict:
                    lhs_industry[l] = entity_dict['industry']
                else:
                    lhs_industry[l] = []
                break
        for r in rhs_cache:
            entity_stream = firebase_db.collection(u'industries').where(u'docid', u'==', r).stream()
            for entity in entity_stream:
                entity_dict = entity.to_dict()
                if 'industry' in entity_dict:
                    rhs_industry[r] = entity_dict['industry']
                else:
                    rhs_industry[r] = []
                break

        #* REFORMAT SYNERGIES INTO ONE LAYER DICTIONARY
        synergies = {}
        weights = {}
        for l in lhs_industry:
            for r in rhs_industry:
                if match_type == 'one2one' or match_type == 'one2many':
                    synergies[r], weights[r] = self.GS.naive_grid(lhs_industry[l], rhs_industry[r])
                elif match_type == 'many2one':
                    synergies[l], weights[r] = self.GS.naive_grid(lhs_industry[l], rhs_industry[r])

        #* Update max weight
        max_weight = -.2
        for w in weights:
            if weights[w] > max_weight:
                max_weight = weights[w]

        return sim_avg, pairs, match_type, to_parent, to_label, synergies, max_weight

    def get_next_g(self, database, firebase_db, userid, from_type, from_value, from_group=[], to_type=[u'startup', u'corporation', u'investor'], ignore=[], num=20):
        '''
        Get the next several matches

        1) Fetch matchCache to see previous matches
        2) Randomly sample from open network
        3) Compute similarity in batches
        4) Update matchCache

        get_next_g(userid, from_type, from_value, from_group, to_type, ignore)
        userid: "userid"
        from_type: "single", "many", "all"
        from_value: [docid] or [docid, docid, ...] NEW
        from_group: [] or [groupid] or [groupid, ...] NEW
        to_type: ["startup", "corporation", "investor"]
        ignore: [docid, docid, ...]
        '''

        #! Fetch history of matches
        user_root = firebase_db.document('matchCache/' + userid)
        root = user_root.get()
        if not root.exists:
            doc = {'timestamp':firestore.SERVER_TIMESTAMP}
            user_root.set(doc)
            root = user_root.get()
        root = root.to_dict()
        print(root)
        if from_type == 'all':
            cache = firebase_db.collection('matchCache/' + userid + '/all')
        elif from_type == 'single':
            cache = firebase_db.collection('matchCache/' + userid + '/' + from_value[0])
        elif from_type == 'many':
            
            if 'cache_map' not in root:
                root['cache_map'] = {}
            
            col_exist = False
            cache_col = ''
            for col in root['cache_map']:
                temp_exist = True
                for group in from_group:
                    if group not in root['cache_map'][col]:
                        temp_exist = False
                for group_ in root['cache_map'][col]:
                    if group_ not in from_group:
                        temp_exist = False
                if temp_exist:
                    col_exist = True
                    cache_col = col
            
            if col_exist:
                cache = firebase_db.collection('matchCache/' + userid + '/' + cache_col)
            else:
                col_id = uuid.uuid4()
                col_id = str(col_id.hex)[0:20]
                new_col = user_root.collection(col_id)
                cache = firebase_db.collection('matchCache/' + userid + '/' + col_id)
                root['cache_map'][col_id] = from_group
        else:
            return "invalid type", 500

        root['timestamp'] = firestore.SERVER_TIMESTAMP
        user_root.update(root)
        
        history_set = set()
        for entity in cache.stream():
            history_set.add(entity.id)

        #! Fetch nodes in open network
        nodes = firebase_db.collection('keywords').where(u'parent', u'==', u'__self__').where(u'label', u'in', to_type)
        node_set = set()
        from_set = set(from_value)
        ignore_set = set(ignore)
        for item in ignore:
            ignore_set.add(item)
        for entity in nodes.stream():
            entity_dict = entity.to_dict()
            if 'docid' in entity_dict:
                node_set.add(entity_dict['docid'])
        
        node_set = node_set.difference(ignore_set)
        # print(node_set)
        node_set = node_set.difference(from_set)
        # print(from_set)
        # print(node_set)
        unexplored = node_set.difference(history_set)
        # print(history_set)
        # print(unexplored)
        # print('explored', len(history_set))
        # print('unexplored', len(unexplored))

        #! Loop until num good matches
        good_matches = 0
        i = 0
        max_iter = 20
        while len(unexplored) > 0 and good_matches < num and i < max_iter:

            #! Randomly select matches
            next_batch = random.sample(unexplored, 1)
            unexplored = unexplored.difference(set(next_batch))
            next_batch = next_batch[0]

            # Match the sample and upload to match cache
            # print('unexplored', len(unexplored))
            # print('exploring', next_batch)
            sim, pairs, match_type, to_parent, to_label, synergies, max_weight = self.fetch_open_db(firebase_db, from_value, next_batch, from_type)

            
            sim += max_weight
            if sim > .99:
                sim = .99
            if sim < .01:
                sim = .01
            print(to_parent, sim, max_weight)
            if sim > 0.6:
                good_matches += 1

            # Update match cache
            batch = firebase_db.batch()
            batch_counter = 0
            if match_type == 'one2one' or match_type == 'one2many':
                
                new_doc = {
                    'score': sim,
                    'to_label': to_label,
                    'to_parent': to_parent,
                    'match_type': match_type,
                    'synergies': synergies,
                    'viewed': False,
                    'timestamp': firestore.SERVER_TIMESTAMP
                }
                child_scores = {}
                for lhs in pairs:
                    for rhs in pairs[lhs]:
                        child_scores[rhs] = pairs[lhs][rhs]['sim']
                        new_doc[rhs] = pairs[lhs][rhs]['words']
                new_doc['child_scores'] = child_scores
                cache.document(to_parent).set(new_doc)
                batch.commit()
            
            elif match_type == 'many2one':

                new_doc = {
                    'score': sim,
                    'to_label': to_label,
                    'to_parent': to_parent,
                    'match_type': match_type,
                    'synergies': synergies,
                    'viewed': False,
                    'timestamp': firestore.SERVER_TIMESTAMP
                }
                child_scores = {}
                for lhs in pairs:
                    for rhs in pairs[lhs]:
                        child_scores[lhs] = pairs[lhs][rhs]['sim']
                        new_doc[lhs] = pairs[lhs][rhs]['words']
                new_doc['child_scores'] = child_scores
                cache.document(to_parent).set(new_doc)
                batch.commit()

            i += 1

        return "success", 200


# x = requests.post('http://localhost:8081/compute_similarity',json=param)
# x = requests.post('http://35.192.131.197/compute_similarity',json=param)