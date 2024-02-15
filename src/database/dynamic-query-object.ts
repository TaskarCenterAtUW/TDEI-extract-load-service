export enum SqlORder {
    ASC = "ASC", DESC = "DESC"
}

export class DynamicQueryObject {
    private _select!: string;
    private _limit = "";
    private _offset = "";
    private _order = "";
    private values: any[] = [];
    paramCouter = 1;

    private conditions: string[] = [];

    /**
    * Push the conditions with placeholder & value. Placeholder counter should be 'paramCouter' of the DynamicQueryObject object.
    */
    condition(clouse: string, value: any) {
        this.conditions.push(clouse);
        if (value instanceof Array) {
            value.forEach(element => {
                this.values.push(element);
            });
        }
        else {
            this.values.push(value);
        }
    }

    buildSelect(tableName: string, columns: string[]) {
        this._select = `SELECT ${columns.join(',')} FROM ${tableName} `;
    }

    buildInnerJoin(sourceTableName: string, destinationTableName: string, joinColumn: string) {
        this._select += ` INNER JOIN ${destinationTableName} on ${destinationTableName}.${joinColumn} = ${sourceTableName}.${joinColumn} `;
    }

    private buildWhere() {
        if (this.conditions.length == 0) return "";
        return ` WHERE ${this.conditions.join(" AND ")}`;
    }
    buildOrder(column: string, order: SqlORder) {
        this._order = ` ORDER BY ${column} ${order.toString()} `;
    }
    buildPagination(page_no: number, page_size: number) {
        //Set defaults if not provided
        if (page_no == undefined || page_no < 1)
            page_no = 1;
        if (page_size == undefined)
            page_size = 10;
        const skip = page_no == 1 ? 0 : (page_no - 1) * page_size;
        const take = page_size > 50 ? 50 : page_size;

        this._limit = ` LIMIT $${this.paramCouter++}`;
        this.values.push(take);
        this._offset = ` OFFSET $${this.paramCouter++} `;
        this.values.push(skip);
    }

    getQuery() {
        return this._select.concat(this.buildWhere()).concat(this._order).concat(this._limit).concat(this._offset);
    }

    getValues() {
        return this.values;
    }
}