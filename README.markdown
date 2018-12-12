[![PyPI version](https://badge.fury.io/py/stepist.svg)](https://badge.fury.io/py/stepist)

Stepist. Framework for data processing.


The main Stepist goal is to simplify working with data.  <br>
<br> 
<br>

**What for:** <br>
- RealTime distributing services 
- ETL tasks
- Prepare data for AI models 


<br>

 
**So, what is Stepist?** <br><br>
This is tool for creating sequence of functions (called steps) which represents execution flow. <br>
The result of each step is input for a next step, as a result you will have graph (data pipeline), 
which could handle data using streaming services (celery, rq, redis) or batch processing tools (kafka).

<br>

###### Basic defenitions:
- **App** - Collect's all your objects and has full configuration of the system. 
- **Step** - Basic object. Connect multiple functions into flow.
- **Flow** - Chain of steps, which start from simple step, and has last step with next_step=None.

<br>

### Examples:

**Simple step by step flow. (result of each step is input for the next)**


```python
from stepist import App

app = App()

@app.step(None)
def step2(a_plus_b, a_minus_b):
    return dict(result=a_plus_b *
    				   a_minus_b)

@app.step(step2)
def step1(a, b):
    return dict(a_plus_b=a+b,
                a_minus_b=a-b)

print(step1(5,5))

```    

<img style='' width=50%;  src="https://github.com/electronick1/stepist/raw/master/static/examples/1.png">

**Simple step by step flow with workers**


```python
import sys
import requests

from stepist import App

app = App()

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@app.step(None)
def step3(text, **kwargs):
    print(text.count('python'))

@app.factory_step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@app.step(step2)
def step1(urls):
    for url in urls:
        yield dict(url=url)

if sys.argv[1] == "worker":
    app.run(step2)  # run worker
else:
    step1(urls=URLS)

# Worker process:
# >>> 94
# >>> 264

```
<img style='' width=50%;  src="https://github.com/electronick1/stepist/raw/master/static/examples/2.png">


**Call multiple steps at once (Map)**

```python
import sys
import requests

from stepist import Hub
from stepist import App

app = App()

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@app.step(None)
def step3(text, **kwargs):
    c = text.count('python')
    return c

@app.factory_step(step3, as_worker=True)
def step2_v2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)
                
@app.factory_step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@app.step(Hub(step2, step2_v2))
def step1(urls):
    for url in urls:
        yield dict(url=url)

if sys.argv[1] == "worker":
    app.run()  # run workers
else:
    print(step1(urls=URLS))

# print, from main process
# >>> [94, 264]
    
```

**Сombine data from multiple steps. (Reduce)**

```python
import sys
import requests

from stepist import Hub
from stepist import App

app = App()

URLS = ['https://www.python.org/',
        'https://wikipedia.org/wiki/Python_(programming_language)']

@app.reducer_step()
def step3(job_list):
    return dict(c1=job_list[0].count('python'),
                c2=job_list[1].count('python'))

@app.factory_step(step3, as_worker=True)
def step2_v2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)
                
@app.factory_step(step3, as_worker=True)
def step2(url):
    r = requests.get(url)
    return dict(url=url,
                text=r.text)

@app.step(Hub(step2, step2_v2))
def step1(urls):
    for url in urls:
        yield dict(url=url)

if sys.argv[1] == "worker":
    app.run()  # run workers
else:
    print(step1(urls=URLS))

# print, from main process
# >>> [94, 264]
    
```

<br> <br> <br>
Stepist Campatible with <a href='http://www.celeryproject.org/'>Celery</a> 

**Celery**
```python

from celery import Celery
from stepist import App
from stepist.flow.workers.adapters.celery_queue import CeleryAdapter

app = App()

celery = Celery(broker="redis://localhost:6379/0")
app.worker_engine = CeleryAdapter(app, celery)


@app.step(None, as_worker=True)
def step3(result):
    return dict(result=result[:2])

@app.step(step3, as_worker=True)
def step2(hello, world):
    return dict(result="%s %s" % (hello, world))

@app.step(step2)
def step1(hello, world):
    return dict(hello=hello.upper(),
                world=world.upper())
       
if __name__ == "__main__":
    print(step1(hello='hello',
                world='world'))
    app.run()                

```

**Custom streaming adapter**
Just define following functions in Base adapter class and assign to app.worker_engine
```python

from stepist import App
from stepist.workers.worker_engine import BaseWorkerEngine


class CustomWorkerEngine(BaseWorkerEngine):

    def add_job(self, step, data, result_reader, **kwargs):
        raise NotImplemented()

    def jobs_count(self, *steps):
        raise NotImplemented()

    def flush_queue(self, step):
        raise NotImplemented()

    def process(self, *steps):
        raise NotImplemented()

    def register_worker(self, handler):
        raise NotImplemented()
        
     
app = App()
app.worker_engine = CustomWorkerEngine()

```
