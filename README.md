# Bee reallocation for Beehive

In Beehive, applications store state in key-value pairs. A tuple (dict, key) is called a cell. Sets of cells that are going to be used together are mapped to a single bee (a lightweight thread of execution), so each cell is managed exclusively by one bee. These bees reside in hives, which are physical machines hosting the Beehive controller.

A message from a switch will be received by the hive that is physically attached to the switch. This message will be mapped to a specific set of cells and sent to the bee(s) in charge.
