# team unknown

everyone left lol - had to do all of this by myself

# pearson's chi square statistic

see: https://web.pdx.edu/~newsomj/pa551/lectur11.htm

$\chi^{2}$ test measures dependence between categorical stochastic variables.

the $\chi^{2}_{t,c}$ value is the lack of independence of term $t$ from category $c$.

$$
\chi_{tc}^2=\frac{N(AD-BC)^2}{(A+B)(A+C)(B+D)(C+D)}
$$

where:

-   $N$ = total number of retrieved documents (can be left out if you only care about ranking order, not scale)
-   $A$ = number of documents that are: in $c$, contain $t$
-   $B$ = number of documents that are: not in $c$, contain $t$
-   $C$ = number of documents that are: in $c$, don't contain $t$
-   $D$ = number of documents that are: not in $c$, don't contain $t$
