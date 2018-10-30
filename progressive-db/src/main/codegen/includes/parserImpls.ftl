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
