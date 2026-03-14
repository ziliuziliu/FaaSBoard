#!/bin/bash
# Default -> 2D Balanced Partitioning (CheckerBoard Partitioning)
# You can add "-strategy <partition_strategy> " to change the graph partition algorithm (refer to FaaSBoard/include/util/flags.h)

# Ensure data directories exist before preprocessing
mkdir -p \
  ../data \
  ../data/livejournal \
  ../data/livejournal/unweighted \
  ../data/livejournal/weighted \
  ../data/livejournal/undirected \
  ../data/twitter \
  ../data/twitter/unweighted \
  ../data/twitter/weighted \
  ../data/twitter/undirected \
  ../data/friendster \
  ../data/friendster/unweighted \
  ../data/friendster/weighted \
  ../data/friendster/undirected \
  ../data/rmat27 \
  ../data/rmat27/unweighted \
  ../data/rmat27/weighted \
  ../data/rmat27/undirected

# directed graph partition for BFS, PR
sudo ../build/preprocess_and_save -graph_file ../data/livejournal.txt -graph_root_dir ../data/livejournal/unweighted -vertices 4847571 -edges 68993773 -partitions 1 --v 1
sudo ../build/preprocess_and_save -graph_file ../data/twitter.txt -graph_root_dir ../data/twitter/unweighted -vertices 41652230 -edges 1468365182 -partitions 6 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/friendster.txt -graph_root_dir ../data/friendster/unweighted -vertices 65608366 -edges 1806067135 -partitions 8 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/rmat27.txt -graph_root_dir ../data/rmat27/unweighted -vertices 134217728 -edges 2147483648 -partitions 9 --v 1

# directed + weighted graph partition for SSSP
# sudo ../build/preprocess_and_save -graph_file ../data/livejournal.txt -graph_root_dir ../data/livejournal/weighted -vertices 4847571 -edges 68993773 -ewT uint32_t -partitions 1 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/twitter.txt -graph_root_dir ../data/twitter/weighted -vertices 41652230 -edges 1468365182 -ewT uint32_t -partitions 12 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/friendster.txt -graph_root_dir ../data/friendster/weighted -vertices 65608366 -edges 1806067135 -ewT uint32_t -partitions 14 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/rmat27.txt -graph_root_dir ../data/rmat27/weighted -vertices 134217728 -edges 2147483648 -ewT uint32_t -partitions 17 --v 1

# undirected graph partition for CC
# sudo ../build/preprocess_and_save -graph_file ../data/livejournal.txt -graph_root_dir ../data/livejournal/undirected -vertices 4847571 -edges 68993773 -undirected=true -partitions 1 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/twitter.txt -graph_root_dir ../data/twitter/undirected -vertices 41652230 -edges 1468365182 -undirected=true -partitions 12 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/friendster.txt -graph_root_dir ../data/friendster/undirected -vertices 65608366 -edges 1806067135 -undirected=true -partitions 14 --v 1
# sudo ../build/preprocess_and_save -graph_file ../data/rmat27.txt -graph_root_dir ../data/rmat27/undirected -vertices 134217728 -edges 2147483648 -undirected=true -partitions 17 --v 1
