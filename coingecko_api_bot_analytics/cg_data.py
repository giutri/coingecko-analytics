#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import requests
import sys
from difflib import SequenceMatcher
from time import sleep


# In[2]:


apiBaseURL = 'https://api.coingecko.com/api/v3/'


# In[3]:


contractAddress = 'OxCONTRACT_ADDRESS'


# In[4]:


def getCoinMarketData(coinId, vs_currency, days, interval):
    url = apiBaseURL+'coins/'+coinId+'/market_chart/?vs_currency='+vs_currency+'&days='+days+'&interval='+interval
    r = requests.get(url)
    
    if (r.status_code == 200):
        res = r.json()
        df = pd.DataFrame(res['prices'])
        df = df.set_index(0)
        df.index = pd.to_datetime(df.index, unit='ms')
        s = df.squeeze()
        
        df2 = pd.DataFrame(res['total_volumes'])
        df2 = df2.set_index(0)
        df2.index = pd.to_datetime(df2.index, unit='ms')
        v = df2.squeeze()
        return s, v
    else:
        print('price data - request error')
        return {}


# In[5]:


def getCoinInfo(coinId):
    url = apiBaseURL+'coins/'+coinId
    r = requests.get(url)
    
    if (r.status_code == 200):
        res = r.json()
        return res
    else:
        print('request error')
        return {}


# In[6]:


def getCoinList():
    url = apiBaseURL+'coins/list'
    r = requests.get(url)
    
    if (r.status_code == 200):
        res = r.json()
        return res
    else:
        print('request error')
        return {}


# In[7]:


def getCoinInfoByAddr(chainId, contractAddress):
    url = apiBaseURL+'coins/'+chainId+'/contract/'+contractAddress
    r = requests.get(url)
    
    if (r.status_code == 200):
        res = r.json()
        return res
    else:
        print('request error')
        return {}


# In[8]:


def getCoinCategories():
    url = apiBaseURL+'coins/categories/list'
    r = requests.get(url)
    
    if (r.status_code == 200):
        res = r.json()
        return res
    else:
        print('request error')
        return {}


# In[9]:


def getTokenInfoFields(e, useFields):
    l = []
    for field in useFields:
        l.append(e[field])
    return pd.Series(l, index=useFields)


# In[10]:


def getImageURL(e):
    return pd.Series([e['image']['large']], index=['image'])


# In[11]:


def getTokenNestedInfo(e, nested_field_name, useNestedFields):
    devData = None
    if (type(e[nested_field_name]) == type([])):
        devData = e[nested_field_name][0]
    else:
        devData = e[nested_field_name]
    l = []
    for field in useNestedFields:
        l.append(devData[field])
    return pd.Series(l, index=useNestedFields)


# In[12]:


def processCoinInfo(e):
    
    useFields = ['id',
     'symbol',
     'name',
     'asset_platform_id',
     'categories',
     'contract_address',
     'sentiment_votes_up_percentage',
     'sentiment_votes_down_percentage',
     'market_cap_rank',
     'coingecko_rank',
     'coingecko_score',
     'developer_score',
     'community_score',
     'liquidity_score',
     'public_interest_score'
    ]

    useDevFields = [
        'forks',
        'stars',
        'subscribers',
        'total_issues',
        'closed_issues',
        'commit_count_4_weeks'
    ]

    useLinkFields = [
        'homepage',
        'blockchain_site',
        'telegram_channel_identifier',
        'twitter_screen_name',
        'facebook_username'
    ]

    useTickerFields = [
        'target',
        'volume',
        'trust_score',
        'is_anomaly',
        'is_stale'
    ]

    useMarketFields = [
        'total_supply',
        'circulating_supply',
        'max_supply',
    ]

    useCommunityFields = [
        'facebook_likes',
        'twitter_followers',
        'reddit_average_posts_48h',
        'reddit_average_comments_48h',
        'reddit_subscribers',
        'reddit_accounts_active_48h',
        'telegram_channel_user_count',
    ]

    s_info = getTokenInfoFields(e, useFields)
    s_dev = getTokenNestedInfo(e, 'developer_data', useDevFields)
    s_ticker = getTokenNestedInfo(e, 'tickers', useTickerFields)
    s_market = getTokenNestedInfo(e, 'market_data', useMarketFields)
    s_community = getTokenNestedInfo(e, 'community_data', useCommunityFields)
    return pd.concat([s_info, s_dev, s_ticker, s_market, s_community])


# In[13]:


def processPriceInfo(priceSeries):
    r = priceSeries.pct_change()
    sk = pd.Series([r.skew(), r.kurtosis()], index=['skew', 'kurt'])
    return pd.concat([r.describe(), sk])


# In[14]:


def combineCoinInfo(e, priceSeries):
    coinInfo = processCoinInfo(e)
    priceInfo = processPriceInfo(priceSeries)
    return pd.concat([coinInfo, priceInfo])


# In[15]:


def getListedCoinNames(field):
    coinList = getCoinList()
    l = []
    for coin in coinList:
        l.append(coin[field].lower())
    return l


# In[16]:


def getCategoryNames():
    categoryList = getCoinCategories()
    l = []
    for cat in categoryList:
        l.append(cat['name'].lower())
    return l


# In[17]:


def getClosestNameMatch(coinList, coinId):
    max_sim = 0
    closestName = ''
    for name in coinList:
        sim = SequenceMatcher(None, coinId, name).ratio()
        if (sim > max_sim):
            max_sim = sim
            closestName = name
    return closestName


# ## run scripts

# In[21]:


def getAllCoinData(coinList, startIdx, endIdx):
    i = startIdx
    for coinId in coinList[startIdx:endIdx]:
        
        if i % 25 == 0:
            print('waiting...')
            sleep(60)
            print('done. continuing...')
            
        try:
            priceSeries, _ = getCoinMarketData(coinId, 'usd', 'max', 'daily')
            otherInfo = getCoinInfo(coinId)
            allInfo = combineCoinInfo(otherInfo, priceSeries)
            allInfo.to_csv('tokenData/'+coinId+'.csv')
            print('Index:', i, '| Id:', coinId, '| Success.')
            
        except:
            print('Index:', i, '| Id:', coinId, '| Error.')
            
        i = i+1


# In[20]:

def getAllPriceData(coinList, startIdx, endIdx):
    i = 0
    for coinId in coinList[startIdx:endIdx]:
        
        if i % 25 == 0:
            print('waiting...')
            sleep(60)
            print('done. continuing...')
            
        try:
            pMax, vMax = getCoinMarketData(coinId, 'usd', 'max', 'daily')
            pNinety, vNinety = getCoinMarketData(coinId, 'usd', '90', 'hourly')
            
            pMax.to_csv('tokenPriceData/'+coinId+'_max_price_daily.csv')
            vMax.to_csv('tokenPriceData/'+coinId+'_max_vol_daily.csv')
            
            pNinety.to_csv('tokenPriceData/'+coinId+'_90_price_hourly.csv')
            vNinety.to_csv('tokenPriceData/'+coinId+'_90_vol_hourly.csv')
            
            print('Index:', i, '| Id:', coinId, '| Success.')
            
        except:
            print('Index:', i, '| Id:', coinId, '| Error.')
            
        i = i+1


# In[ ]:

coinList = getListedCoinNames('id')
sIdx = int(sys.argv[1])
eIdx = int(sys.argv[2])

print(sIdx, eIdx)

if eIdx > len(coinList):
    eIdx = len(coinList)

startIdx = sIdx
endIdx = eIdx

# In[ ]:

print('Getting PRICE data for', str(endIdx - startIdx), ' tokens. Start at: ', startIdx, ' end at', endIdx)
getAllPriceData(coinList, startIdx, endIdx)




