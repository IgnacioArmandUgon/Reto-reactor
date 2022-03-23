package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }

    int contador = 0;
    @Test
    void reactive_filtrarJugadoresMayoresA34(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 34 && player.club.equals("Boca Juniors"))
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    contador = contador + 1;
                    System.out.println(player.getName());
                    System.out.println(contador);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 1;
        System.out.println("listFilter.block().size() = " + listFilter.block().size());
    }

    /*@Test
    void reactive_filtrarRankingDePaises(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 34 && player.club.equals("Boca Juniors"))
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    contador = contador + 1;
                    System.out.println(player.getName());
                    System.out.println(contador);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 1;
        System.out.println("listFilter.block().size() = " + listFilter.block().size());
    }*/
    

    @Test
    void reactive_filtrarRankingDePaises() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .buffer(100)
                .flatMap(player1 -> listFlux
                        .filter(player2 -> player1.stream()
                                .anyMatch(a -> a.national.equals(player2.national)))
                ).distinct()
                .sort((k, player) -> player.winners)
                .collectMultimap(Player::getNational);
        System.out.println("Por Pais: ");
        listFilter.block().forEach((pais, players) -> {
            System.out.println("Pais: " + pais);
            players.forEach(player -> {
                System.out.println("Jugador: " + player.name + " victorias: " + player.winners);
            });
        });
    }



}
