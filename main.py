import math
from datetime import date
from typing import List

import dask.dataframe as dd
import pandas as pd
from fastapi import FastAPI, Query


app = FastAPI()

class Industry(object):
    def __new__(cls):
        if not hasattr(cls, 'industry'):
            cls.industry =  dd.read_csv('data/industry.csv')
        return cls.industry


class BaseData(object):
    def __new__(cls):
        if not hasattr(cls, 'base_data'):
            cls.base_data =  dd.read_csv('data/data.csv')
        return cls.base_data


class Regions(object):
    def __new__(cls):
        if not hasattr(cls, 'regions'):
            cls.regions =  dd.read_csv('data/Regions.csv')
        return cls.regions


class FederalDistrinct(object):
    def __new__(cls):
        if not hasattr(cls, 'federal_distrinct'):
            cls.federal_distrinct =  dd.read_csv('data/Federal_Distrinct.csv')
        return cls.federal_distrinct


def filter_data(Industry_ID, Region_ID, FederalDistrict_ID, GraduationYear,
                Business_Size_ID, Gender, AgeFrom, AgeTo):
    data_response = BaseData()
    age = range((date.today().year - AgeTo), (date.today().year - AgeFrom))
    params = {
        'Industry_ID': Industry_ID,
        'Region_ID': Region_ID,
        'Federal_District_ID': FederalDistrict_ID,
        'Graduation_Year': GraduationYear,
        'Business_Size_ID': Business_Size_ID,
        'Gender': Gender,
        'Birthday': age,
    }
    for key, value in params.items():
        if value:
            data_response = data_response[data_response[key].isin(value)]
    return data_response


@app.get('/api/salaries/avg')
def AVG_salary(
    Industry_ID: List[int] = Query(None),
    Region_ID: List[int] = Query(None),
    Federal_District_ID: List[int] = Query(None),
    GraduationYear: List[int] = Query(None),
    BusinessSize_ID: List[int] = Query(None),
    Gender: List[str] = Query(None),
    AgeFrom: int = 0,
    AgeTo: int = 255,
):
    data = filter_data(Industry_ID, Region_ID, Federal_District_ID,
                       GraduationYear, BusinessSize_ID, Gender, AgeFrom, AgeTo)
    data = data[['Industry_ID', 'Graduates_Amount', 'Salary']]

    # Определение средней зарплаты
    # data2 = data.groupby(['Industry_ID'])['Salary'].mean().reset_index()
    data['Salary'] = data['Salary'] * data['Graduates_Amount']
    data = data.groupby(
        ['Industry_ID'],
        sort=False
        )['Salary', 'Graduates_Amount'].sum().reset_index()
    data['value'] = data['Salary'] / data['Graduates_Amount']

    # объеденение таблиц
    data = dd.merge(Industry(), data, left_on='ID', right_on='Industry_ID')

    # Форматирование данных
    data = data[['ID', 'Industry', 'value']]
    data = data.rename(columns={'ID': 'id', 'Industry': 'name'})
    result = data.nlargest(3, ['value'])

    # Перевод в JSON
    result = result.to_bag(format='dict')
    return result.compute()


@app.get('/api/salaries/distribution')
def distribution(
    Industry_ID: List[int] = Query(None),
    Region_ID: List[int] = Query(None),
    Federal_District_ID: List[int] = Query(None),
    GraduationYear: List[int] = Query(None),
    BusinessSize_ID: List[int] = Query(None),
    Gender: List[str] = Query(None),
    AgeFrom: int = 0,
    AgeTo: int = 255,
):
    data = filter_data(Industry_ID, Region_ID, Federal_District_ID,
                       GraduationYear, BusinessSize_ID, Gender, AgeFrom, AgeTo)
    data = data[['Graduates_Amount', 'Salary']]

    lables = [
        'менее 10 т',
        '10 000 - 15 000 руб.',
        '15 000 - 30 000 руб.',
        '30 000 - 50 000 руб.',
        '50 000 - 80 000 руб.',
        'свыше 80 000',
    ]
    binds = [0, 10000, 15000, 30000, 50000, 80000, math.inf]

    # Разбиение таблицы по Salary
    data['Salary'] = data['Salary'].map_partitions(pd.cut, binds,
                                                   labels=lables)

    data = data.groupby(
        ['Salary'],
        sort=False)['Graduates_Amount'].sum().reset_index()
    count = data['Graduates_Amount'].sum().compute()
    data['percent'] = (data['Graduates_Amount'] / count) * 100

    # Форматирование данных
    data = data.rename(columns={'Salary': 'name', 'Graduates_Amount': 'value'})
    data = data.to_bag(format='dict')

    return data.compute()
