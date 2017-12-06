import "mocha"
import { assert } from "chai"
import apply from '.'
import {FilterBuilder, all, any, eq, find, gt, gte, lt, lte, neq, nfind, prefix, where} from 'querycraft'
import { times, pluck } from 'ramda'
import * as moment from 'moment'

interface TestObject {
    id: string
    _id: number
    bool?: boolean
    num?: number
    text?: string
    obj?: TestObject
    bools?: boolean[]
    nums?: number[]
    texts?: string[]
    objs?: TestObject[]
    things: {id:string, value:number}[]
    date?: Date
}

const now = new Date()

/**
 * Uses prime numbers to define properties of objects so we can use the
 * modulus of the id at certain primes to match the objects
 *
 * @param {number} i
 * @returns {TestObject}
 */
function genTest(i: number): TestObject {
    return {
        id: `${i}`,
        _id: i,
        bool: [undefined, true,false][i%3],
        num: i%5 || undefined,
        text: [undefined, 'foo', 'bar', 'lar', 'lark', 'snark', 'bark'][i%7],
        obj: i%11 === 1 ? genTest((i-1)/11) : undefined,
        bools: i%13 === 0 ? undefined : new Array(i).fill(i%2===0),
        nums: i%17 === 0 ? undefined : new Array(i).fill(null).map((_,j)=>j),
        texts: i%17 === 0 ? undefined : new Array(i)
            .fill(null).map((_,j)=>['foo', 'bar', 'lar'][j%3]),
        objs: i%17 === 0 ? times(genTest, i/17) : undefined,
        things: [{id:'1', value:0}, {id:'2', value:i}, {id:'3', value:-i}],
        date: i%23 === 1 ? moment(now).subtract((i-1)/23, 'days').toDate() : undefined,
    }
}


const testObjects: TestObject[] = times(genTest, 100)
    .sort((a:TestObject,b:TestObject)=> a.id<b.id?1:a.id===b.id?0:-1)

interface ConditionCase {
    conditionName: string
    cases: Array<{
        scenario?: string
        filter: FilterBuilder
        expected: TestObject[]
    }>
}

const conditionCases: ConditionCase[] = [
    {
        conditionName: 'eq',
        cases: [{
            scenario: 'filter a true bool',
            filter: new FilterBuilder()
            .where('bool', eq(true)),
            expected: testObjects.filter(({_id}) => _id%3===1)
        }, {
            scenario: 'filter a non-existing bool',
            filter: new FilterBuilder()
            .where('bool', eq(null)),
            expected: testObjects.filter(({_id}) => _id%3===0)
        }, {
            scenario: 'filter number equals 3',
            filter: new FilterBuilder()
            .where('num', eq(3)),
            expected: testObjects.filter(({_id}) => _id%5===3)
        }, {
            scenario: 'filter empty number',
            filter: new FilterBuilder()
            .where('num', eq(null)),
            expected: testObjects.filter(({_id}) => _id%5===0)
        }, {
            scenario: 'filter text',
            filter: new FilterBuilder()
            .where('text', eq('foo')),
            expected: testObjects.filter(({_id}) => _id%7===1)
        }, {
            scenario: 'filter empty text',
            filter: new FilterBuilder()
            .where('text', eq(null)),
            expected: testObjects.filter(({_id}) => _id%7===0)
        }, {
            scenario: 'filter on sub object',
            filter: new FilterBuilder()
            .where('obj.text', eq('foo')),
            expected: testObjects.filter(({_id}) =>
                _id%11===1&&((_id-1)/11)%7===1)
        }, {
            scenario: 'filter on empty sub object prop',
            filter: new FilterBuilder()
            .where('obj.text', eq(null)),
            expected: testObjects.filter(({_id}) =>
                _id%11!==1 || _id%7===1)
        }]
    }, {
        conditionName: 'neq',
        cases: [{
            scenario: 'filter not a true bool',
            filter: new FilterBuilder()
            .where('bool', neq(true)),
            expected: testObjects.filter(({_id}) => _id%3!==1)
        }, {
            scenario: 'filter a existing bool',
            filter: new FilterBuilder()
            .where('bool', neq(null)),
            expected: testObjects.filter(({_id}) => _id%3!==0)
        }, {
            scenario: 'filter number not equals 3',
            filter: new FilterBuilder()
            .where('num', neq(3)),
            expected: testObjects.filter(({_id}) => _id%5!==3)
        }, {
            scenario: 'filter non-empty number',
            filter: new FilterBuilder()
            .where('num', neq(null)),
            expected: testObjects.filter(({_id}) => _id%5!==0)
        }, {
            scenario: 'filter not eq text',
            filter: new FilterBuilder()
            .where('text', neq('foo')),
            expected: testObjects.filter(({_id}) => _id%7!==1)
        }, {
            scenario: 'filter non-empty text',
            filter: new FilterBuilder()
            .where('text', neq(null)),
            expected: testObjects.filter(({_id}) => _id%7!==0)
        }, {
            scenario: 'filter on sub object (neg)',
            filter: new FilterBuilder()
            .where('obj.text', neq('foo')),
            expected: testObjects.filter(({_id}) =>
                _id%11!==1||((_id-1)/11)%7!==1)
        }, {
            scenario: 'filter on non-empty sub object prop',
            filter: new FilterBuilder()
            .where('obj.text', neq(null)),
            expected: testObjects.filter(({_id}) =>
                _id%11===1&&((_id-1)/11)%7!==0)
        }]
    }, {
        conditionName: 'lt',
        cases: [{
            scenario: 'filter lt number',
            filter: new FilterBuilder()
            .where('num', lt('2')),
            expected: testObjects.filter(({_id}) => _id%5===1)
        }, {
            scenario: 'filter lt string',
            filter: new FilterBuilder()
            .where('text', lt('bark')),
            expected: testObjects.filter(({_id}) => _id%7===2)
        }, {
            scenario: 'filter lt date',
            filter: new FilterBuilder()
            .where('date', lt(now)),
            expected: testObjects.filter(({_id}) => _id>1 && _id%23===1)
        }, {
            scenario: 'filter lt days ago',
            filter: new FilterBuilder()
            .where('date', lt({ daysAgo: 2 })),
            expected: testObjects.filter(({_id}) => (_id-1)/23>2 && _id%23===1)
        }, {
            scenario: 'filter lt null',
            filter: new FilterBuilder()
            .where('num', lt(null)),
            expected: []
        }]
    }, {
        conditionName: 'gt',
        cases: [{
            scenario: 'filter gt number',
            filter: new FilterBuilder()
            .where('num', gt('3')),
            expected: testObjects.filter(({_id}) => _id%5===4)
        }, {
            scenario: 'filter gt string',
            filter: new FilterBuilder()
            .where('text', gt('lark')),
            expected: testObjects.filter(({_id}) => _id%7===5)
        }, {
            scenario: 'filter gt date',
            filter: new FilterBuilder()
            .where('date', gt(now)),
            expected: testObjects.filter(({_id}) => _id%23===1 &&_id<1)
        }, {
            scenario: 'filter gt days ago',
            filter: new FilterBuilder()
            .where('date', gt({ daysAgo: 2 })),
            expected: testObjects.filter(({_id}) => _id%23===1 && (_id-1)/23<2)
        }, {
            scenario: 'filter gt null',
            filter: new FilterBuilder()
            .where('num', gt(null)),
            expected: testObjects.filter(({_id}) => _id%5>0)
        }]
    }, {
        conditionName: 'lte',
        cases: [{
            scenario: 'filter lte number',
            filter: new FilterBuilder()
            .where('num', lte('2')),
            expected: testObjects.filter(({_id}) => _id%5===1||_id%5===2)
        }, {
            scenario: 'filter lte string',
            filter: new FilterBuilder()
            .where('text', lte('bark')),
            expected: testObjects.filter(({_id}) => _id%7===2||_id%7===6)
        }, {
            scenario: 'filter lte date',
            filter: new FilterBuilder()
            .where('date', lte(now)),
            expected: testObjects.filter(({_id}) => _id%23===1 && _id>=1)
        }, {
            scenario: 'filter lte days ago',
            filter: new FilterBuilder()
            .where('date', lte({ daysAgo: 2 })),
            expected: testObjects.filter(({_id}) => _id%23===1 && (_id-1)/23>=2)
        }]
    }, {
        conditionName: 'gte',
        cases: [{
            scenario: 'filter gte number',
            filter: new FilterBuilder()
            .where('num', gte('3')),
            expected: testObjects.filter(({_id}) => _id%5===4 || _id%5===3)
        }, {
            scenario: 'filter gte string',
            filter: new FilterBuilder()
            .where('text', gte('lark')),
            expected: testObjects.filter(({_id}) => _id%7===5 || _id%7===4)
        }, {
            scenario: 'filter gte date',
            filter: new FilterBuilder()
            .where('date', gte(now)),
            expected: testObjects.filter(({_id}) => _id===1)
        }, {
            scenario: 'filter lte days ago',
            filter: new FilterBuilder()
            .where('date', gte({ daysAgo: 2 })),
            expected: testObjects.filter(({_id}) => _id%23===1 && (_id-1)/23<=2)
        }, {
            scenario: 'filter lte null',
            filter: new FilterBuilder()
            .where('num', lte(null)),
            expected: []
        }]
    }, {
        conditionName: 'prefix',
        cases: [{
            scenario: 'filter prefix la',
            filter: new FilterBuilder()
            .where('text', prefix('la')),
            expected: testObjects.filter(({_id}) => _id%7===3||_id%7===4)
        }, {
            scenario: 'filter prefix lar',
            filter: new FilterBuilder()
            .where('text', prefix('lar')),
            expected: testObjects.filter(({_id}) => _id%7===3||_id%7===4)
        }, {
            scenario: 'filter prefix lark',
            filter: new FilterBuilder()
            .where('text', prefix('lark')),
            expected: testObjects.filter(({_id}) => _id%7===4)
        }, {
            scenario: 'filter prefix larks',
            filter: new FilterBuilder()
            .where('text', prefix('larks')),
            expected: []
        }]
    }, {
        conditionName: 'any',
        cases: [{
            scenario: 'any of 2 conditions',
            filter: new FilterBuilder()
            .where('text', any([eq('foo'), eq('bar')])),
            expected: testObjects.filter(({_id}) => _id%7===1||_id%7===2)
        }, {
            scenario: 'any of 3 conditions',
            filter: new FilterBuilder()
            .where('text', any([eq('foo'), eq('bar'),  eq('snark')])),
            expected: testObjects.filter(({_id}) => _id%7===1||_id%7===2||_id%7===5)
        }]
    }, {
        conditionName: 'all',
        cases: [{
            scenario: 'all 2 conditions',
            filter: new FilterBuilder()
            .where('num', all([gt(2), lt(4)])),
            expected: testObjects.filter(({_id}) => _id%5===3)
        }]
    }, {
        conditionName: 'find',
        cases: [{
            scenario: 'filter where can find with a sub obj text=foo',
            filter: new FilterBuilder()
            .where('objs', find(where('text', eq('foo')))),
            expected: testObjects.filter(({_id}) => _id%17===0&&_id/17>1)
        }, {
            scenario: 'filter where can find with a sub obj text=lar',
            filter: new FilterBuilder()
            .where('objs', find(where('text', eq('lar')))),
            expected: testObjects.filter(({_id}) => _id%17===0&&_id/17>3)
        }]
    }, {
        conditionName: 'nfind',
        cases: [{
            scenario: 'filter where cant find with a sub obj text=foo',
            filter: new FilterBuilder()
            .where('objs', nfind(where('text', eq('foo')))),
            expected: testObjects.filter(({_id}) => _id%17!==0||_id/17<=1)
        }, {
            scenario: 'filter where cant find with a sub obj text=lar',
            filter: new FilterBuilder()
            .where('objs', nfind(where('text', eq('lar')))),
            expected: testObjects.filter(({_id}) => _id%17!==0||_id/17<=3)
        }]
    }
]

function getIds(list: TestObject[]){
    return list.map(item => item.id)
}

describe('toFunction', function(){

    it('should make no changes on empty queries', function(){
        const filteredObjects = apply<TestObject>(new FilterBuilder(), [])
        assert.deepEqual(filteredObjects, [])
    })

    it('should return a new array', function(){
        const filteredObjects = apply<TestObject>(new FilterBuilder(), testObjects)
        assert.notEqual(filteredObjects, testObjects)
    })


    it('should allow filtering on multiple fields', function(){
        const filter = new FilterBuilder()
            .where('text', eq('foo'))
            .where ('num', eq(1))
            .where ('bool', eq(true))

        const expected = testObjects.filter(({_id}) =>
            _id%7===1 && _id%5===1 && _id%3===1)

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.sameMembers(getIds(filteredObjects), getIds(expected))
    })

    it('should respect sort direction', function(){
        const filterASC = new FilterBuilder()
            .setSortDirection('ASC')
        const filterDESC = new FilterBuilder()
            .setSortDirection('DESC')

        const filteredObjectsASC = apply<TestObject>(filterASC, testObjects)
        const filteredObjectsDESC = apply<TestObject>(filterDESC, testObjects)
        assert.deepEqual(getIds(filteredObjectsASC), getIds(testObjects.reverse()))
        assert.deepEqual(getIds(filteredObjectsDESC), getIds(testObjects.reverse()))
    })


    it('should support sorting on array fields', function(){
        const filter = new FilterBuilder()
            .setSortFieldId('things', '2', 'value')
            .setSortDirection('DESC')

        const expected = testObjects
            .sort((a,b)=>a._id<b._id?1:a._id===b._id?0:-1)

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.deepEqual(getIds(filteredObjects), getIds(expected))
    })

    it('should respect limits', function(){
        const filter = new FilterBuilder()
            .setLimit(Math.floor(testObjects.length/5))

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.equal(filteredObjects.length, Math.floor(testObjects.length/5))
    })

    it('should respect "or"', function(){
        const filter = new FilterBuilder()
            .where('text', eq('foo'))
            .or()
            .where ('num', eq(1))

        const expected = testObjects.filter(({_id}) =>
            _id%7===1 || _id%5===1)

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.sameMembers(getIds(filteredObjects), getIds(expected))
    })

    it('should respect "and"', function(){
        const filter = new FilterBuilder()
            .where('text', eq('foo'))
            .and()
            .where ('num', eq(1))

        const expected = testObjects.filter(({_id}) =>
            _id%7===1 && _id%5===1)

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.sameMembers(getIds(filteredObjects), getIds(expected))
    })

    it('should respect "and" with "or"', function(){
        const filter = new FilterBuilder()
                .where('text', eq('foo'))
                .or()
                .where ('num', eq(1))
                .or()
                .where ('num', eq(2))
            .and()
                .where ('bool', eq(true))

        const expected = testObjects.filter(({_id}) =>
            (_id%7===1 || _id%5===1 || _id%5===2) && _id%3===1)

        const filteredObjects = apply<TestObject>(filter, testObjects)
        assert.sameMembers(getIds(filteredObjects), getIds(expected))
    })

    for (let {conditionName, cases} of conditionCases){
        describe(`${conditionName} condition`, function(){
            for (let {filter, expected, scenario} of cases){
                it(`should ${scenario}`, function(){
                    const filteredObjects = apply<TestObject>(filter, testObjects)
                    assert.sameMembers(getIds(filteredObjects), getIds(expected))
                })
            }
        })
    }
})