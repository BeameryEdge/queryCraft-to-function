import { FilterBuilder, QueryBuilder, Condition, Datum, OrderCondition, Value } from 'querycraft'
import { map, isEmpty, complement, contains, tap, chain, any, all, anyPass, allPass, split, sort, identity, path, propEq } from 'ramda'
import * as moment from 'moment'
/**
 * Get the expanded set of values at an index for an object.
 * i.e. flatMap across arrays so an AND condition on <listProp>.<prop>
 * returns the the <prop> object from each item in <listProp>
 * on the object. where as <prop>.<listProp> will access the
 * return each item in the list at <listProp> on <prop>
 *
 * @param {string[]} path
 * @param {*} obj
 * @returns {any[]}
 */
function get(path: string[], obj: any): any[]{
    if (Array.isArray(obj)) return chain(($obj: any) => get(path, $obj), obj)
    if (obj == null) return []
    if (path.length === 0) return [obj]
    return get(path.slice(1), obj[path[0]])
}

/**
 * Ensure an object is toLowerCase, if it is a string
 *
 * @param {*} value
 * @returns
 */
function toLowerCase(value: any){
    return value //typeof value === 'string' ? value.toLowerCase() : value
}

/**
 * Create a predicate function f::(T=>boolean) from a query
 *
 * @param {QueryBuilder} query
 * @returns
 */
function queryToFunction<T>(query: QueryBuilder){
    // for all field conditions
    return (obj:T) => all(fieldId => {
        // check that the condition holds
        const condition = query.getFieldCondition(fieldId);
        return condition ? check(fieldId, condition, obj) : true
    }, query.getFieldIds())
}

/**
 *  Check a field condition on an object
 *
 * @param {string} fieldId
 * @param {Condition} condition
 * @param {*} obj
 * @returns
 */
function check(fieldId: string, condition: Condition, obj: any) {
    return conditionToFunction(condition)(get(fieldId.split(/\./g), obj).map(toLowerCase))
}

function testOrderCondition(condition: OrderCondition, testValue: Value){
    if (testValue == null) {
        if (condition.op === 'LT') return testValue != null
        if (condition.op === 'GT') return false
        if (condition.op === 'LTE') return true
        if (condition.op === 'GTE') return testValue == null
    } else if (condition.value == null) {
        if (condition.op === 'LT') return false
        if (condition.op === 'GT') return testValue != null
        if (condition.op === 'LTE') return testValue == null
        if (condition.op === 'GTE') return true
    } else if (typeof condition.value === 'boolean' || typeof condition.value === 'number' || condition.value instanceof Date || typeof condition.value === 'string') {
        if (condition.op === 'LT') return testValue < condition.value
        if (condition.op === 'GT') return testValue > condition.value
        if (condition.op === 'LTE') return testValue <= condition.value
        if (condition.op === 'GTE') return testValue >= condition.value
    } else {
        const date = moment(testValue as number)
        const bound = moment().subtract(condition.value.daysAgo, 'days')
        if (condition.op === 'LT') return date.isBefore(bound, 'days')
        if (condition.op === 'GT') return date.isAfter(bound, 'days')
        if (condition.op === 'LTE') return date.isSameOrBefore(bound, 'days')
        if (condition.op === 'GTE') return date.isSameOrAfter(bound, 'days')
    }
    throw new TypeError('Expected OrderCondition got ' + typeof condition)
}


/**
 * Convert a condition to a function on a list of values
 *
 * @param {Condition} condition
 * @returns {(values: any) => boolean}
 */
function conditionToFunction(condition: Condition): (values: any) => boolean {
    switch (condition.op) {
        case 'EQ':
            return condition.value === null ?
                all(value => value === null || value === undefined || value === '') :
                any(value => toLowerCase(value) === toLowerCase(condition.value))
        case 'NEQ':
            return condition.value === null ?
                any(value => value !== null && value !== undefined && value !== '') :
                all(value => toLowerCase(value) !== toLowerCase(condition.value))
        case 'LT':  case 'GT': case 'LTE': case 'GTE':
            return any<Value>(value => {
                return testOrderCondition(condition, value)
            })
        case 'ALL':
            return allPass(condition.value.map(conditionToFunction))
        case 'ANY':
            return anyPass(condition.value.map(conditionToFunction))
        case 'PREFIX':
            return any(value => typeof value === 'string' ? value.slice(0, condition.value.length) === condition.value : false)
        case 'FIND':
            return any(queryToFunction(condition.value))
        case 'NFIND':
            return complement(any(queryToFunction(condition.value)))
        default:
            throw new Error('Cannot generate query function for: ' + typeof condition)
    }
}

/**
 * Apply a filter::Filter<T> object to an Array of objects input::T[]
 *
 * @export
 * @param {FilterBuilder} filter
 * @param {T[]} input
 * @returns {T[]}
 */
export default function apply<T extends { id: string }>(filter: FilterBuilder, input: T[]): T[] {
    const sortFieldSubId = filter.getSortFieldSubId();
    const sortFieldSubProp = filter.getSortFieldSubProp();
    const sortFieldId = filter.getSortFieldId();
    const sortDirection = filter.getSortDirection() === 'ASC' ? 1 : -1
    const getSortVal: (datum: Datum) => Value = datum => {
        const sortPath = sortFieldId.split(/\./g)
        const sortVal = path<Value | Datum[]>(sortPath, datum)
        if (sortFieldSubId && sortFieldSubProp) {
            if (sortVal && Array.isArray(sortVal) && sortVal.length) {
                return path<Value>(sortFieldSubProp.split(/\./g), (sortVal as Datum[]).find(propEq('id', sortFieldSubId)))
            } else {
                return null;
            }
        } else if (Array.isArray(sortVal)) {
            return null
        }
        return sortVal;
    }

    const sortFn = sortFieldId ?
        //sort by sortFieldId
            (a: any, b: any) => {
                let [aVal, bVal] = [a,b].map(getSortVal)
                if (aVal == null && bVal == null) return a.id > b.id ? sortDirection : - sortDirection
                if (aVal == null) return 1
                if (bVal == null) return -1
                if (aVal > bVal || aVal == bVal && a.id > b.id) return sortDirection
                else return -sortDirection
            } :
        //dont sort
        () => 0

    // for ALL alternatives sets we assert that ANY query must pass
    const filterFn = allPass(map(anyPass, map(map(queryToFunction), filter.getStatements())))

    return input
    .filter(filterFn)
    .sort(sortFn)
    .slice(0, filter.getLimit())
}