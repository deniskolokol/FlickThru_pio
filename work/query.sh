#!/usr/bin/env bash

echo ""
echo "============ simple user recs ============"
echo ""
echo "Recommendations for user: usr1"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "user": "usr1"
}' http://localhost:8000/queries.json
echo ""


echo ""
echo "Recommendations for user: usr9"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "user": "usr9"
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "Recommendations for usr9 with negative bias"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "user": "usr9",
    "event": "dislike",
    "bias": -1
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "============ simple similar item recs ============"
echo ""
echo "Recommendations for item"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "item": "Base London Lancelot Leather Boots (6087778)"
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "============ popular item recs only ============"
echo ""
echo "query with no item or user id, ordered by popularity"
echo ""
curl -H "Content-Type: application/json" -d '
{
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "Recommendations for non-existant user: xyz, all from popularity"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "user": "xyz"
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "Recommendations for non-existant item: xyz, all from popularity"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "item": "xyz"
}' http://localhost:8000/queries.json
echo ""

echo ""
echo "Recommendations for no user no item, all from popularity, Jewellery filter"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "fields": [{
        "name": "category",
        "values": ["Jewellery"],
        "bias": -1
    }]
}' http://localhost:8000/queries.json
echo ""


echo ""
echo "Recommendations for no user no item, all from popularity, Jewellery boost"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "fields": [{
        "name": "categories",
        "values": ["Jewellery"],
        "bias": 1.05
    }]
}' http://localhost:8000/queries.json
echo ""


echo ""
echo "Recommendations for no user no item, all from popularity, Jewellery boost, filter by gender"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "fields": [{
        "name": "category",
        "values": ["Jewellery"],
        "bias": 1.05
    }, {
        "name": "gender",
        "values": ["women"],
        "bias": -1
    }]
}' http://localhost:8000/queries.json
echo ""


echo ""
echo "============ dateRange filter ============"
echo ""
if [[ "$OSTYPE" == "linux-gnu" ]]; then
  BEFORE=`date --date="tomorrow" --iso-8601=seconds`
  AFTER=`date --date="1 day ago" --iso-8601=seconds`
else
  BEFORE=`date -v +1d +"%Y-%m-%dT%H:%M:%SZ"`
  AFTER=`date -v -1d +"%Y-%m-%dT%H:%M:%SZ"`
fi
#echo "before: $BEFORE after: $AFTER"
echo "Recommendations for user: usr1"
echo ""
curl -H "Content-Type: application/json" -d "
{
    \"user\": \"usr1\",
    \"dateRange\": {
        \"name\": \"date\",
        \"before\": \"$BEFORE\",
        \"after\": \"$AFTER\"
    }
}" http://localhost:8000/queries.json
echo ""

echo ""
echo "============ query with item and user *EXPERIMENTAL* ============"
# This is experimental, use at your own risk, not well founded in theory
echo ""
echo "Recommendations for no user no item, all from popularity, Tablets boost, Estados Unidos Mexicanos filter"
echo ""
curl -H "Content-Type: application/json" -d '
{
    "user": "usr9",
    "item": ""Base London Lancelot Leather Boots (6087778)"
}' http://localhost:8000/queries.json
echo ""
