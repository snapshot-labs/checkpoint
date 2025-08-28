export const spaceDeployedEvent = {
  data: [
    '0x6abd599ab530c5b3bc603111bdd20d77890db330402dc870fc9866f50ed6d2a',
    '0x56ecf84acc36d7d878ab11067c2e9870a38f10d0819c698f8c1f559c40d3a',
    '0x0',
    '0x0',
    '0x15180',
    '0x1',
    '0x0',
    '0x6abd599ab530c5b3bc603111bdd20d77890db330402dc870fc9866f50ed6d2a',
    '0x1',
    '0x0',
    '0x2',
    '0xd1b81feff3095ca9517fdfc7427e742ce96f7ca8f3b2664a21b2fba552493b',
    '0xd1b81feff3095ca9517fdfc7427e742ce96f7ca8f3b2664a21b2fba552493b',
    '0x7',
    '0x2',
    '0x0',
    '0x2',
    '0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6',
    '0x3',
    '0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6',
    '0x3',
    '0x2',
    '0x5e1f273ca9a11f78bfb291cbe1b49294cf3c76dd48951e7ab7db6d9fb1e7d62',
    '0x64cce9272197eba6353f5bbf060e097e516b411e66e83a9cf5910a08697df14',
    '0x1',
    '0x4ecc83848a519cc22b0d0ffb70e65ec8dde85d3d13439eff7145d4063cf6b4d',
    '0x3',
    '0x697066733a2f2f6261666b726569617978776969726337666e6b7461796d62',
    '0x34367363356162767a6f6c6f6864343666717a6d71616b6e6f337467716c36',
    '0x32686669'
  ],
  from_address:
    '0xe1e511e496a72791ab3d591ba7d571a32de4261d84e4d183f26b6325970e20',
  keys: ['0xfb483ab6758cfd02170a30e08181f6e7397c1a32c2966ce3a8c4702b7ec142']
};

export const voteCreatedEvent = {
  data: [
    '0x8',
    '0xef8305e140ac520225daf050e2f71d5fbcc543e7',
    '0x1',
    '0x1',
    '0x0'
  ],
  from_address:
    '0x750118894bf8b3ad7fd79763899c4528b2a7a0c17d7185dff78eaddbff2cb0b',
  keys: ['0x35a0a3a79d25118031c4960817fe040fe30a9d229c30e63c993a5bfee52d32b']
};

export const spaceDeployedEventCairo1 = {
  block_hash:
    '0x8c069e00a34ae275efe05b4b5f9395d615f9caf003c4a0d0576c225a95d673',
  block_number: 114,
  data: [
    '0x7b52be32f53235445c94247942b516cb9f8ace110c82ca8a72af527eb7d44b0',
    '0x40279a6371314c37464ebc327856a2d332b89022109f55b9ee93179bbbe727a'
  ],
  from_address:
    '0x6838e761bc2e07c9563251a03a60b9a013f2d36b73246fc9d850932ac519696',
  keys: ['0x2d8cd3e2509f757328c6abf59278c05024d30ae28426655f886b32be3eeaa9f'],
  transaction_hash:
    '0x569e2dac76352ea03888e9ff3135374e87124324ce22ce98ab5d16f922cc33e'
};

export const spaceFactoryAbi = [
  {
    name: 'Uint256',
    size: 2,
    type: 'struct',
    members: [
      {
        name: 'low',
        type: 'felt',
        offset: 0
      },
      {
        name: 'high',
        type: 'felt',
        offset: 1
      }
    ]
  },
  {
    data: [
      {
        name: 'deployer_address',
        type: 'felt'
      },
      {
        name: 'space_address',
        type: 'felt'
      },
      {
        name: 'voting_delay',
        type: 'felt'
      },
      {
        name: 'min_voting_duration',
        type: 'felt'
      },
      {
        name: 'max_voting_duration',
        type: 'felt'
      },
      {
        name: 'proposal_threshold',
        type: 'Uint256'
      },
      {
        name: 'controller',
        type: 'felt'
      },
      {
        name: 'quorum',
        type: 'Uint256'
      },
      {
        name: 'voting_strategies_len',
        type: 'felt'
      },
      {
        name: 'voting_strategies',
        type: 'felt*'
      },
      {
        name: 'voting_strategy_params_flat_len',
        type: 'felt'
      },
      {
        name: 'voting_strategy_params_flat',
        type: 'felt*'
      },
      {
        name: 'authenticators_len',
        type: 'felt'
      },
      {
        name: 'authenticators',
        type: 'felt*'
      },
      {
        name: 'execution_strategies_len',
        type: 'felt'
      },
      {
        name: 'execution_strategies',
        type: 'felt*'
      },
      {
        name: 'metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'metadata_uri',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'space_deployed',
    type: 'event'
  },
  {
    name: 'constructor',
    type: 'constructor',
    inputs: [
      {
        name: 'space_class_hash',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'deploySpace',
    type: 'function',
    inputs: [
      {
        name: 'public_key',
        type: 'felt'
      },
      {
        name: 'voting_delay',
        type: 'felt'
      },
      {
        name: 'min_voting_duration',
        type: 'felt'
      },
      {
        name: 'max_voting_duration',
        type: 'felt'
      },
      {
        name: 'proposal_threshold',
        type: 'Uint256'
      },
      {
        name: 'controller',
        type: 'felt'
      },
      {
        name: 'quorum',
        type: 'Uint256'
      },
      {
        name: 'voting_strategies_len',
        type: 'felt'
      },
      {
        name: 'voting_strategies',
        type: 'felt*'
      },
      {
        name: 'voting_strategy_params_flat_len',
        type: 'felt'
      },
      {
        name: 'voting_strategy_params_flat',
        type: 'felt*'
      },
      {
        name: 'authenticators_len',
        type: 'felt'
      },
      {
        name: 'authenticators',
        type: 'felt*'
      },
      {
        name: 'execution_strategies_len',
        type: 'felt'
      },
      {
        name: 'execution_strategies',
        type: 'felt*'
      },
      {
        name: 'metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'metadata_uri',
        type: 'felt*'
      }
    ],
    outputs: []
  }
];

export const spaceAbi = [
  {
    name: 'Address',
    size: 1,
    type: 'struct',
    members: [
      {
        name: 'value',
        type: 'felt',
        offset: 0
      }
    ]
  },
  {
    name: 'Proposal',
    size: 5,
    type: 'struct',
    members: [
      {
        name: 'quorum',
        type: 'Uint256',
        offset: 0
      },
      {
        name: 'timestamps',
        type: 'felt',
        offset: 2
      },
      {
        name: 'execution_strategy',
        type: 'felt',
        offset: 3
      },
      {
        name: 'execution_hash',
        type: 'felt',
        offset: 4
      }
    ]
  },
  {
    name: 'Uint256',
    size: 2,
    type: 'struct',
    members: [
      {
        name: 'low',
        type: 'felt',
        offset: 0
      },
      {
        name: 'high',
        type: 'felt',
        offset: 1
      }
    ]
  },
  {
    name: 'Vote',
    size: 3,
    type: 'struct',
    members: [
      {
        name: 'choice',
        type: 'felt',
        offset: 0
      },
      {
        name: 'voting_power',
        type: 'Uint256',
        offset: 1
      }
    ]
  },
  {
    name: 'AccountCallArray',
    size: 4,
    type: 'struct',
    members: [
      {
        name: 'to',
        type: 'felt',
        offset: 0
      },
      {
        name: 'selector',
        type: 'felt',
        offset: 1
      },
      {
        name: 'data_offset',
        type: 'felt',
        offset: 2
      },
      {
        name: 'data_len',
        type: 'felt',
        offset: 3
      }
    ]
  },
  {
    name: 'ProposalInfo',
    size: 11,
    type: 'struct',
    members: [
      {
        name: 'proposal',
        type: 'Proposal',
        offset: 0
      },
      {
        name: 'power_for',
        type: 'Uint256',
        offset: 5
      },
      {
        name: 'power_against',
        type: 'Uint256',
        offset: 7
      },
      {
        name: 'power_abstain',
        type: 'Uint256',
        offset: 9
      }
    ]
  },
  {
    data: [
      {
        name: 'previousOwner',
        type: 'felt'
      },
      {
        name: 'newOwner',
        type: 'felt'
      }
    ],
    keys: [],
    name: 'OwnershipTransferred',
    type: 'event'
  },
  {
    data: [
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'proposer_address',
        type: 'Address'
      },
      {
        name: 'proposal',
        type: 'Proposal'
      },
      {
        name: 'metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'metadata_uri',
        type: 'felt*'
      },
      {
        name: 'execution_params_len',
        type: 'felt'
      },
      {
        name: 'execution_params',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'proposal_created',
    type: 'event'
  },
  {
    data: [
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'voter_address',
        type: 'Address'
      },
      {
        name: 'vote',
        type: 'Vote'
      }
    ],
    keys: [],
    name: 'vote_created',
    type: 'event'
  },
  {
    data: [
      {
        name: 'previous',
        type: 'Uint256'
      },
      {
        name: 'new_quorum',
        type: 'Uint256'
      }
    ],
    keys: [],
    name: 'quorum_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'previous',
        type: 'felt'
      },
      {
        name: 'new_voting_delay',
        type: 'felt'
      }
    ],
    keys: [],
    name: 'voting_delay_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'previous',
        type: 'felt'
      },
      {
        name: 'new_voting_duration',
        type: 'felt'
      }
    ],
    keys: [],
    name: 'min_voting_duration_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'previous',
        type: 'felt'
      },
      {
        name: 'new_voting_duration',
        type: 'felt'
      }
    ],
    keys: [],
    name: 'max_voting_duration_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'previous',
        type: 'Uint256'
      },
      {
        name: 'new_proposal_threshold',
        type: 'Uint256'
      }
    ],
    keys: [],
    name: 'proposal_threshold_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'new_metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'new_metadata_uri',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'metadata_uri_updated',
    type: 'event'
  },
  {
    data: [
      {
        name: 'added_len',
        type: 'felt'
      },
      {
        name: 'added',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'authenticators_added',
    type: 'event'
  },
  {
    data: [
      {
        name: 'removed_len',
        type: 'felt'
      },
      {
        name: 'removed',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'authenticators_removed',
    type: 'event'
  },
  {
    data: [
      {
        name: 'added_len',
        type: 'felt'
      },
      {
        name: 'added',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'execution_strategies_added',
    type: 'event'
  },
  {
    data: [
      {
        name: 'removed_len',
        type: 'felt'
      },
      {
        name: 'removed',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'execution_strategies_removed',
    type: 'event'
  },
  {
    data: [
      {
        name: 'added_len',
        type: 'felt'
      },
      {
        name: 'added',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'voting_strategies_added',
    type: 'event'
  },
  {
    data: [
      {
        name: 'removed_len',
        type: 'felt'
      },
      {
        name: 'removed',
        type: 'felt*'
      }
    ],
    keys: [],
    name: 'voting_strategies_removed',
    type: 'event'
  },
  {
    name: 'constructor',
    type: 'constructor',
    inputs: [
      {
        name: 'public_key',
        type: 'felt'
      },
      {
        name: 'voting_delay',
        type: 'felt'
      },
      {
        name: 'min_voting_duration',
        type: 'felt'
      },
      {
        name: 'max_voting_duration',
        type: 'felt'
      },
      {
        name: 'proposal_threshold',
        type: 'Uint256'
      },
      {
        name: 'controller',
        type: 'felt'
      },
      {
        name: 'quorum',
        type: 'Uint256'
      },
      {
        name: 'voting_strategies_len',
        type: 'felt'
      },
      {
        name: 'voting_strategies',
        type: 'felt*'
      },
      {
        name: 'voting_strategy_params_flat_len',
        type: 'felt'
      },
      {
        name: 'voting_strategy_params_flat',
        type: 'felt*'
      },
      {
        name: 'authenticators_len',
        type: 'felt'
      },
      {
        name: 'authenticators',
        type: 'felt*'
      },
      {
        name: 'execution_strategies_len',
        type: 'felt'
      },
      {
        name: 'execution_strategies',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'getPublicKey',
    type: 'function',
    inputs: [],
    outputs: [
      {
        name: 'publicKey',
        type: 'felt'
      }
    ],
    stateMutability: 'view'
  },
  {
    name: 'supportsInterface',
    type: 'function',
    inputs: [
      {
        name: 'interfaceId',
        type: 'felt'
      }
    ],
    outputs: [
      {
        name: 'success',
        type: 'felt'
      }
    ],
    stateMutability: 'view'
  },
  {
    name: 'setPublicKey',
    type: 'function',
    inputs: [
      {
        name: 'newPublicKey',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'isValidSignature',
    type: 'function',
    inputs: [
      {
        name: 'hash',
        type: 'felt'
      },
      {
        name: 'signature_len',
        type: 'felt'
      },
      {
        name: 'signature',
        type: 'felt*'
      }
    ],
    outputs: [
      {
        name: 'isValid',
        type: 'felt'
      }
    ],
    stateMutability: 'view'
  },
  {
    name: '__validate__',
    type: 'function',
    inputs: [
      {
        name: 'call_array_len',
        type: 'felt'
      },
      {
        name: 'call_array',
        type: 'AccountCallArray*'
      },
      {
        name: 'calldata_len',
        type: 'felt'
      },
      {
        name: 'calldata',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: '__validate_declare__',
    type: 'function',
    inputs: [
      {
        name: 'class_hash',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: '__validate_deploy__',
    type: 'function',
    inputs: [
      {
        name: 'class_hash',
        type: 'felt'
      },
      {
        name: 'salt',
        type: 'felt'
      },
      {
        name: 'publicKey',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: '__execute__',
    type: 'function',
    inputs: [
      {
        name: 'call_array_len',
        type: 'felt'
      },
      {
        name: 'call_array',
        type: 'AccountCallArray*'
      },
      {
        name: 'calldata_len',
        type: 'felt'
      },
      {
        name: 'calldata',
        type: 'felt*'
      }
    ],
    outputs: [
      {
        name: 'response_len',
        type: 'felt'
      },
      {
        name: 'response',
        type: 'felt*'
      }
    ]
  },
  {
    name: 'propose',
    type: 'function',
    inputs: [
      {
        name: 'proposer_address',
        type: 'Address'
      },
      {
        name: 'metadata_uri_string_len',
        type: 'felt'
      },
      {
        name: 'metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'metadata_uri',
        type: 'felt*'
      },
      {
        name: 'execution_strategy',
        type: 'felt'
      },
      {
        name: 'used_voting_strategies_len',
        type: 'felt'
      },
      {
        name: 'used_voting_strategies',
        type: 'felt*'
      },
      {
        name: 'user_voting_strategy_params_flat_len',
        type: 'felt'
      },
      {
        name: 'user_voting_strategy_params_flat',
        type: 'felt*'
      },
      {
        name: 'execution_params_len',
        type: 'felt'
      },
      {
        name: 'execution_params',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'vote',
    type: 'function',
    inputs: [
      {
        name: 'voter_address',
        type: 'Address'
      },
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'choice',
        type: 'felt'
      },
      {
        name: 'used_voting_strategies_len',
        type: 'felt'
      },
      {
        name: 'used_voting_strategies',
        type: 'felt*'
      },
      {
        name: 'user_voting_strategy_params_flat_len',
        type: 'felt'
      },
      {
        name: 'user_voting_strategy_params_flat',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'finalizeProposal',
    type: 'function',
    inputs: [
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'execution_params_len',
        type: 'felt'
      },
      {
        name: 'execution_params',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'cancelProposal',
    type: 'function',
    inputs: [
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'execution_params_len',
        type: 'felt'
      },
      {
        name: 'execution_params',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'hasVoted',
    type: 'function',
    inputs: [
      {
        name: 'proposal_id',
        type: 'felt'
      },
      {
        name: 'voter_address',
        type: 'Address'
      }
    ],
    outputs: [
      {
        name: 'voted',
        type: 'felt'
      }
    ],
    stateMutability: 'view'
  },
  {
    name: 'getProposalInfo',
    type: 'function',
    inputs: [
      {
        name: 'proposal_id',
        type: 'felt'
      }
    ],
    outputs: [
      {
        name: 'proposal_info',
        type: 'ProposalInfo'
      }
    ],
    stateMutability: 'view'
  },
  {
    name: 'setController',
    type: 'function',
    inputs: [
      {
        name: 'new_controller',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'setQuorum',
    type: 'function',
    inputs: [
      {
        name: 'new_quorum',
        type: 'Uint256'
      }
    ],
    outputs: []
  },
  {
    name: 'setVotingDelay',
    type: 'function',
    inputs: [
      {
        name: 'new_delay',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'setMinVotingDuration',
    type: 'function',
    inputs: [
      {
        name: 'new_min_voting_duration',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'setMaxVotingDuration',
    type: 'function',
    inputs: [
      {
        name: 'new_max_voting_duration',
        type: 'felt'
      }
    ],
    outputs: []
  },
  {
    name: 'setProposalThreshold',
    type: 'function',
    inputs: [
      {
        name: 'new_proposal_threshold',
        type: 'Uint256'
      }
    ],
    outputs: []
  },
  {
    name: 'setMetadataUri',
    type: 'function',
    inputs: [
      {
        name: 'new_metadata_uri_len',
        type: 'felt'
      },
      {
        name: 'new_metadata_uri',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'addExecutionStrategies',
    type: 'function',
    inputs: [
      {
        name: 'addresses_len',
        type: 'felt'
      },
      {
        name: 'addresses',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'removeExecutionStrategies',
    type: 'function',
    inputs: [
      {
        name: 'addresses_len',
        type: 'felt'
      },
      {
        name: 'addresses',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'addVotingStrategies',
    type: 'function',
    inputs: [
      {
        name: 'addresses_len',
        type: 'felt'
      },
      {
        name: 'addresses',
        type: 'felt*'
      },
      {
        name: 'params_flat_len',
        type: 'felt'
      },
      {
        name: 'params_flat',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'removeVotingStrategies',
    type: 'function',
    inputs: [
      {
        name: 'indexes_len',
        type: 'felt'
      },
      {
        name: 'indexes',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'addAuthenticators',
    type: 'function',
    inputs: [
      {
        name: 'addresses_len',
        type: 'felt'
      },
      {
        name: 'addresses',
        type: 'felt*'
      }
    ],
    outputs: []
  },
  {
    name: 'removeAuthenticators',
    type: 'function',
    inputs: [
      {
        name: 'addresses_len',
        type: 'felt'
      },
      {
        name: 'addresses',
        type: 'felt*'
      }
    ],
    outputs: []
  }
];

export const factoryAbiCairo1 = [
  {
    type: 'impl',
    name: 'Factory',
    interface_name: 'sx::factory::factory::IFactory'
  },
  {
    type: 'struct',
    name: 'core::array::Span::<core::felt252>',
    members: [
      {
        name: 'snapshot',
        type: '@core::array::Array::<core::felt252>'
      }
    ]
  },
  {
    type: 'interface',
    name: 'sx::factory::factory::IFactory',
    items: [
      {
        type: 'function',
        name: 'deploy',
        inputs: [
          {
            name: 'class_hash',
            type: 'core::starknet::class_hash::ClassHash'
          },
          {
            name: 'contract_address_salt',
            type: 'core::felt252'
          },
          {
            name: 'initialize_calldata',
            type: 'core::array::Span::<core::felt252>'
          }
        ],
        outputs: [
          {
            type: 'core::starknet::contract_address::ContractAddress'
          }
        ],
        state_mutability: 'external'
      }
    ]
  },
  {
    type: 'event',
    name: 'sx::factory::factory::Factory::SpaceDeployed',
    kind: 'struct',
    members: [
      {
        name: 'class_hash',
        type: 'core::starknet::class_hash::ClassHash',
        kind: 'data'
      },
      {
        name: 'space_address',
        type: 'core::starknet::contract_address::ContractAddress',
        kind: 'data'
      }
    ]
  },
  {
    type: 'event',
    name: 'sx::factory::factory::Factory::Event',
    kind: 'enum',
    variants: [
      {
        name: 'SpaceDeployed',
        type: 'sx::factory::factory::Factory::SpaceDeployed',
        kind: 'nested'
      }
    ]
  }
];
