# QueryCraft-To-Function-To-Function
Converts a [QueryCraft](https://github.com/BeameryHQ/QueryCraft) Filter Builder object into a function to filter arrays of objects.


[![NPM](https://nodei.co/npm/querycraft-to-function.png)](https://npmjs.org/package/querycraft-to-function)

[![npm version](https://badge.fury.io/js/querycraft-to-function.svg)](https://badge.fury.io/js/querycraft-to-function)
[![CircleCI](https://circleci.com/gh/BeameryHQ/QueryCraft-To-Function.svg?style=shield)](https://circleci.com/gh/BeameryHQ/QueryCraft-To-Function)
[![codecov](https://codecov.io/gh/BeameryHQ/QueryCraft-To-Function/branch/master/graph/badge.svg)](https://codecov.io/gh/BeameryHQ/QueryCraft-To-Function)
[![David deps](https://david-dm.org/BeameryHQ/QueryCraft-To-Function.svg)](https://david-dm.org/BeameryHQ/QueryCraft-To-Function)
[![Known Vulnerabilities](https://snyk.io/test/github/beameryhq/querycraft-to-function/badge.svg)](https://snyk.io/test/github/beameryhq/querycraft-to-function)

## Installation

```sh
npm install --save 'querycraft'
npm install --save 'querycraft-to-function'
```

## Examples

Suppose we have a collection of data that satisfies the interface

```ts
interface contact {
    id: string
    'list': { id: string }[]
    firstName: string
    lastName: string
    email: string
    createdAt: Date
    customFields: { id: string, value: number }[]
    assignedTo?: string
}
```

If we want a query the describes the logic:-
```
    first 50 items where
        fistName is bob
        lastName is doyle OR is not set
        assignedTo is anything
        list has an item where id is item1
    sorted (in ascending order) by the value property of the customField where id is custom1
    created less than 5 days ago
```

We can build build it as easily as:-

```ts
import { FilterBuilder, eq, lt, neq, any, find, where, BucketsAggregation,  } from 'querycraft'
import { apply, ArraySource } from 'querycraft-to-function'

const contacts: contact[] =  [ ... ]

const filter = new FilterBuilder()
.where('firstName', eq('bob'))
.where('list', find(where('id', eq('item1'))))
.where('lastName', any([
    eq('doyle'),
    eq(null)
]))
.where('createdAt', lt({ daysAgo: 5 }))
.where('assignedTo', neq(null))
.setSortFieldId('customFields', 'custom1', 'value')
.setSortDirection('ASC')
.setLimit(50)

console.log(apply(filter, contacts))
// -> filtered list of contacts

```

If we would instead for example want to get contacts named bob
grouped by the day they were created :-

```ts
import { FilterAggregation, eq, BucketsAggregation,  } from 'querycraft'
import { apply, ArraySource } from 'querycraft-to-function'

const buckets = new ArraySource(testObjects)
    .pipe(new FilterAggregation())
        .where('firstName', eq('bob'))
    .pipe(new BucketsAggregation({
        fieldId: 'createdAt',
        dateInterval: 'day'
    }))
    .sink()
// -> grouped counts of contacts
```
