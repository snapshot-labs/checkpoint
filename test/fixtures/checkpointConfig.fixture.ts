export const validCheckpointConfig = {
  network_node_url: 'https://starknet-goerli.infura.io/v3/SOME_KEY',
  sources: [
    {
      contract:
        '0x0625dc1290b6e936be5f1a3e963cf629326b1f4dfd5a56738dea98e1ad31b7f3',
      start: 112319,
      events: [
        {
          name: 'proposal_created',
          fn: 'handlePropose'
        },
        {
          name: 'vote_created',
          fn: 'handleVote'
        }
      ]
    }
  ]
};
