fetch('/graphql', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    },
    body: JSON.stringify({query: `
        {
          readNode(path: ["db1", "tb1"]) {
            id_
            subNode {
              ... on Children {
                nodes {
                  id_
                  subNode {
                    ... on Children {
                      nodes {
                        id_
                        subNode {
                          ... on IntValue {
                            intItem: item
                          }
                        }
                      }
                    }
                    ... on IntValue {
                      intItem: item
                    }
                    ... on FloatValue {
                      floatItem: item
                    }
                    ... on StrValue {
                      strItem: item
                    }
                  }
                }
              }
              ... on IntValue {
                intItem: item
              }
              ... on FloatValue {
                floatItem: item
              }
              ... on StrValue {
                strItem: item
              }
            }
          }
        }
  `})
})
  .then(r => r.json())
  .then(data => console.log('data returned:', data));
