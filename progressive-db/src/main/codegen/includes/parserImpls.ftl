SqlPrepareTable SqlPrepareTable() :
{
    final SqlIdentifier id;
    final Span s;
}
{
    <PREPARE> <TABLE> id = CompoundIdentifier() {
        s = span();
        return new SqlPrepareTable(s.end(id), id);
    }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlNode ProgressiveOrderedQueryOrExpr() :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = ProgressiveQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw new ParseException(RESOURCE.limitStartCountNotAllowed().str());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}

SqlNode FutureOrderedQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = FutureQueryOrExpr(exprContext)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw new ParseException(RESOURCE.limitStartCountNotAllowed().str());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}

SqlNode ProgressiveQueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = ProgressiveLeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = ProgressiveLeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

SqlNode FutureQueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = FutureLeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = FutureLeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

SqlNode ProgressiveLeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = Expression(exprContext) { return e; }
|
    e = ProgressiveLeafQuery(exprContext) { return e; }
}

SqlNode FutureLeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = Expression(exprContext) { return e; }
|
    e = FutureLeafQuery(exprContext) { return e; }
}

SqlNode ProgressiveLeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = SqlSelectProgressive() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}

SqlNode FutureLeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = SqlFutureSelect() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}

SqlSelectProgressive SqlSelectProgressive() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList withFutureGroupBy;
    final SqlNodeList groupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final Span s;
}
{
    <SELECT> <PROGRESSIVE>
    {
        s = span();
    }
    SqlSelectKeywords(keywords)
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectFutureList()
    (
        <FROM> fromClause = FromClause()
        where = WhereOpt()
        withFutureGroupBy = WithFutureGroupByOpt()
        groupBy = GroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            withFutureGroupBy = null;
            groupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelectProgressive(s.end(this), keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            fromClause, where, withFutureGroupBy, groupBy, having,
            windowDecls, null, null, null);
    }
}

SqlSelect SqlFutureSelect() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList futureGroupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final Span s;
}
{
    <SELECT>
    {
        s = span();
    }
    SqlSelectKeywords(keywords)
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectFutureList()
    (
        <FROM> fromClause = FromClause()
        where = WhereOpt()
        futureGroupBy = FutureGroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            futureGroupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelect(s.end(this), keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            fromClause, where, futureGroupBy, having, windowDecls, null, null, null);
    }
}

SqlNodeList WithFutureGroupByOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    <WITH> <FUTURE> <GROUP> { s = span(); }
    <BY> list = GroupingElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

List<SqlNode> SelectFutureList() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode item;
}
{
    item = SelectFutureItem() {
        list.add(item);
    }
    (
        <COMMA> item = SelectFutureItem() {
            list.add(item);
        }
    )*
    {
        return list;
    }
}

SqlNode SelectFutureItem() :
{
    SqlNode e;
    SqlIdentifier id;
}
{
    e = SelectExpression()
    [
        <FUTURE> {
            e = new SqlFutureNode(e, getPos());
        }
    ]
    [
        [ <AS> ]
        id = SimpleIdentifier() {
            e = SqlStdOperatorTable.AS.createCall(span().end(e), e, id);
        }
    ]
    {
        return e;
    }
}

SqlNodeList FutureGroupByOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    <GROUP> { s = span(); }
    <BY> list = FutureGroupingElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

List<SqlNode> FutureGroupingElementList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    e = FutureGroupingElement() { list.add(e); }
    (
        <COMMA>
        e = FutureGroupingElement() { list.add(e); }
    )*
    { return list; }
}

SqlNode FutureGroupingElement() :
{
    List<SqlNode> list;
    final SqlNodeList nodes;
    final SqlNode e;
    final Span s;
}
{
    LOOKAHEAD(3)
    <LPAREN> <RPAREN> {
        return new SqlNodeList(getPos());
    }
|   e = Expression(ExprContext.ACCEPT_SUB_QUERY) <FUTURE> {
        return new SqlFutureNode(e, getPos());
    }
|   e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        return e;
    }
}

SqlCreate SqlCreateProgressiveView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <PROGRESSIVE> <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = FutureOrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlCreateProgressiveView(s.end(this), replace, id, columnList, query);
    }
}

SqlDrop SqlDropProgressiveView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <PROGRESSIVE> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new SqlDropProgressiveView(s.end(this), ifExists, id);
    }
}

