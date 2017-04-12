
Stepist. Framework for data processing.


(Deep alpha) <br>
Right now, Python 3.0+ only


The main Stepist goal - simplify working with data.  <br>
Stepist provide distributing computing and infrastructure to easily control all your data calculations. 
<br>
<br>

**What for:** <br><br>
	- RealTime distributing services 
    - Background distributing computing 
    - Prepare data for AI models 
    - Prepare data for analytic 



 
**So, what is Stepist?** <br><br>
This is tool for creating sequence of functions (called steps) which represents execution flow. <br>
The result of each step is input for a next step, and in the total it create graph of whole data processing flow.

<br>

###### Basic defenitions:
- **Step** - Basic object. Connect multiple function into flow.
- **Flow** - Chain of steps, which start from simple step, and has last step with next_step=None. (result values from last step is result for flow)

<br>

### Examples:

###### Simple step by step flow. (result of each step is input for the next)


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

<img style='' width=50%;  src="https://github.com/electronick1/stepist/raw/master/static/examples/1.png">

###### Simple step by step flow with workers


```python
import sys
import requests
from stepist.flow import step, run, factory_step

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@step(None)
def step3(text, **kwargs):
    print(text.count('python'))

@factory_step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@step(step2)
def step1(urls):
    print("urls")
    return [dict(url=url) for url in urls]

if sys.argv[1] == "worker":
    run(step2)  # run worker
else:
    step1(urls=URLS)

# Worker process:
# >>> 94
# >>> 264

```
<img style='' width=50%;  src="https://github.com/electronick1/stepist/raw/master/static/examples/2.png">


######  Connecting multiple flows with workers

```python
import sys
import requests
from stepist.flow import step, run, factory_step

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@step(None)
def step3(text, **kwargs):
    c = text.count('python')
    return c

@factory_step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@step(None, next_flow=step2)
def step1(urls, next_flow):
    for url in urls:
        next_flow.add_item(dict(url=url))

    return list(next_flow.result())

if sys.argv[1] == "worker":
    run(step2)  # run worker
else:
    print(step1(urls=URLS))

# print, from main process
# >>> [94, 264]
    
```
<img style='' width=50%;  src="https://github.com/electronick1/stepist/raw/master/static/examples/3.png">
