
Stepist. 


(Deep alpha) <br>
Right now, Python 3.0+ only


The main Stepist goal - simplify working with data.  <br>
Stepist provide distributing computing and infrastructure to easily control all your data calculations. 
<br>
<br>

###### What for:
	- RealTime distributing services 
    - Background distributing computing 
    - Prepare data for AI models 
    - Prepare data for analytic 
    - Simple assistant for fast data processing


 
###### So, what is Stepist? 
This is tool for creating sequence of functions (called steps) which represents execution flow. <br>
The result of each step is input for a next step, and in the total it create graph of whole data processing flow.

<br>

### Examples:

###### Simple step by step flow. (result of each step is input for the next)


<img style='float:right; margin-top:30px' width=300px;  src="https://github.com/electronick1/stepist/raw/master/static/examples/1.png">

```python
from stepist.flow import step

@step(None)
def step2(a_plus_b, a_minus_b):
    return dict(result=a_plus_b *
    				   a_minus_b)

@step(step2)
def step1(a, b):
    return dict(a_plus_b=a+b,
                a_minus_b=a-b)

print(step1(5,5))
# >>> 0
```    

###### Simple step by step flow with workers

```python
import sys
import requests
from stepist.flow import step, run

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@step(None)
def step3(text, **kwargs):
    print(text.count('python'))

@step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@step(step2)
def step1(url):
    # DO what you want with url
    return dict(url=url)


if sys.argv[1] == "worker":
    run(step2)  # run worker
else:
    for url in URLS:
        step1(url=url)


```
